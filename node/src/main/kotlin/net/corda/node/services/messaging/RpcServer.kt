package net.corda.node.services.messaging

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.pool.KryoPool
import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import com.google.common.cache.RemovalListener
import com.google.common.collect.HashMultimap
import com.google.common.util.concurrent.SettableFuture
import net.corda.core.ErrorOr
import net.corda.core.ThreadBox
import net.corda.core.crypto.commonName
import net.corda.core.messaging.RPCOps
import net.corda.core.random63BitValue
import net.corda.core.serialization.KryoPoolWithContext
import net.corda.core.utilities.debug
import net.corda.core.utilities.loggerFor
import net.corda.node.services.RPCUserService
import net.corda.nodeapi.ArtemisMessagingComponent.Companion.NODE_USER
import net.corda.nodeapi.RPCException
import net.corda.nodeapi.RPCKryo
import net.corda.nodeapi.RpcApi
import net.corda.nodeapi.User
import org.apache.activemq.artemis.api.core.Message
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.api.core.client.ClientConsumer
import org.apache.activemq.artemis.api.core.client.ClientMessage
import org.apache.activemq.artemis.api.core.client.ClientProducer
import org.apache.activemq.artemis.api.core.client.ClientSession
import org.bouncycastle.asn1.x500.X500Name
import rx.Notification
import rx.Observable
import rx.Subscriber
import rx.Subscription
import java.lang.reflect.InvocationTargetException
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

data class RpcServerConfiguration(
        val reapIntervalMs: Long  /** The interval of subscription reaping in milliseconds */
) {
    companion object {
        val default = RpcServerConfiguration(
                reapIntervalMs = 1000
        )
    }
}

/**
 * The [RpcServer] implements the complement of [RpcClient]. When an RPC request arrives it dispatches to the
 * corresponding function in [ops]. During serialisation of the reply (and later observations) the server subscribes to
 * each Observable it encounters and captures the client address to associate with these Observables. Later it uses this
 * address to forward observations arriving on the Observables.
 *
 * The way this is done is similar to that in [RpcClient], we use Kryo and add a context to stores the subscription map.
 */
class RpcServer(
        private val ops: RPCOps,
        private val executor: ScheduledExecutorService,
        private val artemisState: ThreadBox<ArtemisState>,
        private val userService: RPCUserService,
        private val nodeLegalName: String,
        private val rpcConfiguration: RpcServerConfiguration = RpcServerConfiguration.default
) {
    data class ArtemisState(
            val producerSession: ClientSession,
            val consumerPool: List<ClientConsumer>,
            val producer: ClientProducer
    )
    private companion object {
        val log = loggerFor<RpcServer>()
        val kryoPool = KryoPool.Builder { RPCKryo(RpcServerObservableSerializer) }.build()
    }
    // The methodname->Method map to use for dispatching.
    private val methodTable = ops.javaClass.declaredMethods.groupBy { it.name }.mapValues { it.value.single() }
    // The observable subscription mapping.
    private val observableMap = createObservableSubscriptionMap()
    // The scheduled reaper handle.
    private lateinit var reaperScheduledFuture: ScheduledFuture<*>

    private fun createObservableSubscriptionMap(): ObservableSubscriptionMap {
        val onObservableRemove = RemovalListener<RpcApi.ObservableId, ObservableSubscription> {
            log.debug { "Unsubscribing from Observable with id ${it.key} because of ${it.cause}" }
            it.value.subscription.unsubscribe()
        }
        return CacheBuilder.newBuilder().removalListener(onObservableRemove).build()
    }

    fun start() {
        reaperScheduledFuture = executor.scheduleAtFixedRate(
                this::reapSubscriptions,
                rpcConfiguration.reapIntervalMs,
                rpcConfiguration.reapIntervalMs,
                TimeUnit.SECONDS
        )
        artemisState.locked {
            consumerPool.forEach { it.setMessageHandler(this@RpcServer::artemisMessageHandler) }
        }
    }

    fun close() {
        reaperScheduledFuture.cancel(false)
        observableMap.invalidateAll()
        reapSubscriptions()
    }

    private fun artemisMessageHandler(artemisMessage: ClientMessage) {
        log.debug("Message handler called")
        val clientToServer = RpcApi.ClientToServer.fromClientMessage(executor, kryoPool, artemisMessage)
        log.debug { "Got message from RPC client $clientToServer" }
        clientToServer.accept(
                onRpcRequest = { rpcRequest ->
                    val result = ErrorOr.catch {
                        val rpcContext = RpcContext(
                                currentUser = getUser(artemisMessage)
                        )
                        try {
                            CURRENT_RPC_CONTEXT.set(rpcContext)
                            val method = methodTable[rpcRequest.methodName] ?:
                                    throw RPCException("Received RPC for unknown method ${rpcRequest.methodName} - possible client/server version skew?")
                            method.invoke(ops, *rpcRequest.arguments.toTypedArray())
                        } finally {
                            CURRENT_RPC_CONTEXT.remove()
                        }
                    }
                    val resultWithExceptionUnwrapped = result.mapError {
                        if (it is InvocationTargetException) {
                            it.cause ?: RPCException("Caught InvocationTargetException without cause")
                        } else {
                            it
                        }
                    }
                    val reply = RpcApi.ServerToClient.RpcReply(
                            id = rpcRequest.id,
                            result = resultWithExceptionUnwrapped
                    )
                    val observableContext = ObservableContext(observableMap, rpcRequest.clientAddress, artemisState, executor, kryoPool)
                    observableContext.sendMessageBlocking(reply)
                },
                onObservablesClosed = {
                    observableMap.invalidateAll(it.ids)
                }
        )
        artemisMessage.acknowledge()
        log.debug("Returning from message handler")
    }

    private fun reapSubscriptions() {
        log.debug("Server reaping")
        // TODO collect these asynchronously rather than by polling
        val clientAddressToObservable = HashMultimap.create<SimpleString, RpcApi.ObservableId>()
        observableMap.asMap().forEach {
            clientAddressToObservable.put(it.value.clientAddress, it.key)
        }
        artemisState.locked {
            clientAddressToObservable.asMap().forEach { (address, observables) ->
                val queryResult = producerSession.queueQuery(address)
                if (queryResult == null || queryResult.consumerCount == 0) {
                    observableMap.invalidateAll(observables)
                }
            }
        }
        observableMap.cleanUp()
        log.debug("Server reaping done")
    }

    // TODO remove this User once webserver doesn't need it
    private val nodeUser = User(NODE_USER, NODE_USER, setOf())
    private fun getUser(message: ClientMessage): User {
        val validatedUser = message.getStringProperty(Message.HDR_VALIDATED_USER) ?: throw IllegalArgumentException("Missing validated user from the Artemis message")
        val rpcUser = userService.getUser(validatedUser)
        if (rpcUser != null) {
            return rpcUser
        } else if (X500Name(validatedUser).commonName == nodeLegalName) {
            return nodeUser
        } else {
            throw IllegalArgumentException("Validated user '$validatedUser' is not an RPC user nor the NODE user")
        }
    }
}

@JvmField
internal val CURRENT_RPC_CONTEXT: ThreadLocal<RpcContext> = ThreadLocal()
val rpcContext: RpcContext get() = CURRENT_RPC_CONTEXT.get()

/**
 * @param currentUser This is available to RPC implementations to query the validated [User] that is calling it. Each
 *     user has a set of permissions they're entitled to which can be used to control access.
 */
data class RpcContext(
        val currentUser: User
)

private class ObservableSubscription(
        val clientAddress: SimpleString,
        val subscription: Subscription
)

private typealias ObservableSubscriptionMap = Cache<RpcApi.ObservableId, ObservableSubscription>

// We construct an observable context on each RPC request. If subsequently a nested Observable is
// encountered this same context is propagated by the instrumented KryoPool. This way all
// observations rooted in a single RPC will be muxed correctly. Note that the context construction
// itself is quite cheap.
private class ObservableContext(
        val observableMap: ObservableSubscriptionMap,
        val clientAddress: SimpleString,
        val artemisState: ThreadBox<RpcServer.ArtemisState>,
        val executor: ExecutorService,
        kryoPool: KryoPool
) {
    // We create a message queue for serialisation because during serialisation we may encounter
    // Observables that instantly emit events once subscribed to, which would trigger
    // serialisation-within-serialisation, and in our case deadlock. Instead we queue up the message
    // and process the queue in a loop until it's empty.
    private val messageQueue = LinkedList<Pair<RpcApi.ServerToClient, SettableFuture<Unit>?>>()
    // We use a CAS boolean to denote whether we are already serialising (and hence the message should just be queued or
    // we should start the serialisation.
    private val sending = AtomicBoolean(false)
    private val kryoPoolWithObservableContext = RpcServerObservableSerializer.createPoolWithContext(kryoPool, this)
    fun sendMessageBlocking(serverToClient: RpcApi.ServerToClient) {
        val completeFuture = SettableFuture.create<Unit>()
        synchronized(messageQueue) { messageQueue.add(serverToClient to completeFuture) }
        handleMessagesInQueue()
        completeFuture.get()
    }

    fun sendMessageNonBlocking(serverToClient: RpcApi.ServerToClient) {
        synchronized(messageQueue) { messageQueue.add(serverToClient to null) }
        handleMessagesInQueue()
    }

    private fun handleMessagesInQueue() {
        if (sending.compareAndSet(false, true)) {
            while (true) {
                // We need to couple [messageQueue] and [sending] to avoid missing any messages that may arrive from
                // other threads.
                val nextMessage = synchronized(messageQueue) {
                    val nextMessage = messageQueue.pollFirst()
                    if (nextMessage == null) {
                        sending.set(false)
                    }
                    nextMessage
                } ?: break
                val artemisMessage = artemisState.locked { artemisState.content.producerSession.createMessage(false) }
                nextMessage.first.writeToClientMessage(executor, kryoPoolWithObservableContext, artemisMessage)
                executor.submit {
                    artemisState.locked { producer.send(clientAddress, artemisMessage) }
                }
                nextMessage.second?.set(Unit)
            }
        }
    }
}

private object RpcServerObservableSerializer : Serializer<Observable<Any>>() {
    private object RpcObservableContextKey
    private val log = loggerFor<RpcServerObservableSerializer>()

    fun createPoolWithContext(kryoPool: KryoPool, observableContext: ObservableContext): KryoPool {
        return KryoPoolWithContext(kryoPool, RpcObservableContextKey, observableContext)
    }

    override fun read(kryo: Kryo?, input: Input?, type: Class<Observable<Any>>?): Observable<Any> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun write(kryo: Kryo, output: Output, observable: Observable<Any>) {
        val observableId = RpcApi.ObservableId(random63BitValue())
        val observableContext = kryo.context[RpcObservableContextKey] as ObservableContext
        output.writeLong(observableId.toLong, true)
        val observableWithSubscription = ObservableSubscription(
                clientAddress = observableContext.clientAddress,
                // We capture [observableContext] in the subscriber. Note that all synchronisation/kryo borrowing
                // must be done again within the subscriber
                subscription = observable.materialize().subscribe(
                        object : Subscriber<Notification<Any>>() {
                            override fun onNext(observation: Notification<Any>) {
                                if (!isUnsubscribed) {
                                    observableContext.sendMessageNonBlocking(RpcApi.ServerToClient.Observation(observableId, observation))
                                }
                            }
                            override fun onError(exception: Throwable) {
                                log.error("onError called in materialize()d RPC Observable", exception)
                            }
                            override fun onCompleted() {
                            }
                        }
                )
        )
        observableContext.observableMap.put(observableId, observableWithSubscription)
    }
}