package net.corda.client.rpc

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.pool.KryoPool
import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import com.google.common.cache.RemovalCause
import com.google.common.cache.RemovalListener
import com.google.common.util.concurrent.SettableFuture
import com.google.common.util.concurrent.ThreadFactoryBuilder
import net.corda.core.ThreadBox
import net.corda.core.getOrThrow
import net.corda.core.pinInSubscriptions
import net.corda.core.random63BitValue
import net.corda.core.serialization.KryoPoolWithContext
import net.corda.core.utilities.loggerFor
import net.corda.nodeapi.RPCKryo
import net.corda.nodeapi.RpcApi
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.api.core.client.*
import rx.Notification
import rx.Observable
import rx.subjects.UnicastSubject
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

/**
 * This class provides a proxy implementation of an RPC interface for RPC clients. It translates API calls to lower-level
 * RPC protocol messages. For this protocol see [RpcApi].
 *
 * When a method is called on the interface the arguments are serialised and the request is forwarded to the node. The
 * server then executes the code that implements the RPC and sends a reply.
 *
 * An RPC reply may contain [Observable]s, which are serialised simply as unique IDs. On the client side we create a
 * [UnicastSubject] for each such ID. Subsequently the server may send observations attached to this ID, which are
 * forwarded to the [UnicastSubject]. Note that the observations themselves may contain further [Observable]s, which are
 * handled in the same way.
 *
 * To do the above we take advantage of Kryo's datastructure traversal. When the client is deserialising a message from
 * the server that may contain Observables it is supplied with an [ObservableContext] that exposes the map used to demux
 * the observations. When an [Observable] is encountered during traversal a new [UnicastSubject] is added to the map and
 * we carry on. Each observation later contains the corresponding Observable ID, and we just forward that to the
 * associated [UnicastSubject].
 *
 * The client may signal that it no longer consumes a particular [Observable]. This may be done explicitly by
 * unsubscribing from the [Observable], or if the [Observable] is garbage collected the client will eventually
 * automatically signal the server. This is done using a cache that holds weak references to the [UnicastSubject]s.
 * The cleanup happens in batches using a dedicated reaper, scheduled on [executor].
 */
internal class RpcClientProxyHandler(
        private val rpcConfiguration: RpcClientConfiguration,
        private val artemisState: ThreadBox<ArtemisState>,
        private val clientAddress: SimpleString
) : InvocationHandler {
    data class ArtemisState(
            val session: ClientSession,
            val producer: ClientProducer,
            val consumer: ClientConsumer
    )

    private companion object {
        val log = loggerFor<RpcClientProxyHandler>()
        // Note that this KryoPool is not yet capable of deserialising Observables, it requires Proxy-specific context
        // to do that. However it may still be used for serialisation of RPC requests and related messages.
        val kryoPool = KryoPool.Builder { RPCKryo(RpcClientObservableSerializer) }.build()
    }

    private val executor = Executors.newScheduledThreadPool(
            rpcConfiguration.threadPoolSize,
            ThreadFactoryBuilder().setNameFormat("rpc-client-proxy-pool-%d").build()
    )

    // Holds the RPC reply futures.
    private val rpcReplyMap = ConcurrentHashMap<RpcApi.RpcRequestId, SettableFuture<Any?>>()
    // Holds the Observables.
    private val observableContext = ObservableContext(
            observableMap = createRpcObservableMap(),
            hardReferenceStore = Collections.synchronizedSet(mutableSetOf<Observable<*>>())
    )
    // Holds a reference to the scheduled reaper.
    private lateinit var reaperScheduledFuture: ScheduledFuture<*>

    // Stores the [Observable] IDs that are already removed from the map but are not yet sent to the server.
    private val observablesToReap = Collections.synchronizedList(ArrayList<RpcApi.ObservableId>())
    // A Kryo pool that automatically adds the observable context when an instance is requested.
    private val kryoPoolWithObservableContext = RpcClientObservableSerializer.createPoolWithContext(kryoPool, observableContext)

    private fun createRpcObservableMap(): RpcObservableMap {
        val onObservableRemove = RemovalListener<RpcApi.ObservableId, UnicastSubject<Notification<Any>>> {
            if (it.cause == RemovalCause.COLLECTED) {
                log.warn("Observable was never subscribed to, scheduling for reaping")
            }
            observablesToReap.add(it.key)
        }
        return CacheBuilder.newBuilder().weakValues().removalListener(onObservableRemove).build()
    }

    /**
     * Starts the Artemis session associated with the proxy handler as well as the reaper.
     */
    fun start() {
        reaperScheduledFuture = executor.scheduleAtFixedRate(
                this::reapObservables,
                rpcConfiguration.reapIntervalMs,
                rpcConfiguration.reapIntervalMs,
                TimeUnit.MILLISECONDS
        )
        artemisState.locked {
            consumer.setMessageHandler(this@RpcClientProxyHandler::artemisMessageHandler)
            session.start()
        }
    }

    // This is the general function that transforms a client side RPC to internal Artemis messages.
    override fun invoke(proxy: Any, method: Method, arguments: Array<out Any?>?): Any? {
        val rpcId = RpcApi.RpcRequestId(random63BitValue())
        val request = RpcApi.ClientToServer.RpcRequest(clientAddress, rpcId, method.name, arguments?.toList() ?: emptyList())
        val message = artemisState.locked { session.createMessage(false) }
        request.writeToClientMessage(executor, kryoPool, message)
        log.debug("Sending RPC request ${method.name}")
        val replyFuture = SettableFuture.create<Any>()
        require(rpcReplyMap.put(rpcId, replyFuture) == null) {
            "Generated several RPC requests with same ID $rpcId"
        }
        artemisState.locked { producer.send(message) }
        return replyFuture.getOrThrow()
    }

    private fun artemisMessageHandler(message: ClientMessage) {
        val serverToClient = RpcApi.ServerToClient.fromClientMessage(executor, kryoPoolWithObservableContext, message)
        log.info("Got message from RPC server $serverToClient")
        serverToClient.accept(
                onRpcReply = {
                    val replyFuture = rpcReplyMap.remove(it.id)
                    if (replyFuture == null) {
                        log.error("RPC reply arrived to unknown RPC ID ${it.id}, this indicates an internal RPC error.")
                        return@accept
                    }
                    it.result.match(
                            onError = { replyFuture.setException(it) },
                            onValue = { replyFuture.set(it) }
                    )
                },
                onObservation = {
                    val observable = observableContext.observableMap.getIfPresent(it.id)
                    if (observable == null) {
                        log.warn("Observation arrived to unknown Observable with ID ${it.id}. " +
                                "This may be due to an observation arriving before the server was " +
                                "notified of observable shutdown")
                        return@accept
                    }
                    observable.onNext(it.content)
                    if (it.content.isOnCompleted || it.content.isOnError) {
                        observableContext.observableMap.invalidate(it.id)
                    }
                }
        )
        log.info("Message processed")
    }

    private fun reapObservables() {
        observableContext.observableMap.cleanUp()
        val toReap = observablesToReap
        if (toReap.isNotEmpty()) {
            log.info("Reaping ${observablesToReap.size} observables")
            artemisState.locked {
                val message = session.createMessage(false)
                RpcApi.ClientToServer.ObservablesClosed(observablesToReap).writeToClientMessage(message)
                observablesToReap.clear()
                producer.send(message)
            }
        }
    }

    fun close() {
        reaperScheduledFuture.cancel(false)
        observableContext.observableMap.invalidateAll()
        reapObservables()
        executor.shutdown()
    }
}

private typealias RpcObservableMap = Cache<RpcApi.ObservableId, UnicastSubject<Notification<Any>>>

/**
 * Holds a context available during Kryo deserialisation of messages that are expected to contain Observables.
 *
 * @param observableMap holds the Observables that are ultimately exposed to the user.
 * @param hardReferenceStore holds references to Observables we want to keep alive while they are subscribed to.
 */
private data class ObservableContext(
        val observableMap: RpcObservableMap,
        val hardReferenceStore: MutableSet<Observable<*>>
)

/**
 * A [Serializer] to deserialise Observables once the corresponding Kryo instance has been provided with an
 * [ObservableContext].
 */
private object RpcClientObservableSerializer : Serializer<Observable<Any>>() {
    private object RpcObservableContextKey
    fun createPoolWithContext(kryoPool: KryoPool, observableContext: ObservableContext): KryoPool {
        return KryoPoolWithContext(kryoPool, RpcObservableContextKey, observableContext)
    }

    override fun read(kryo: Kryo, input: Input, type: Class<Observable<Any>>): Observable<Any> {
        @Suppress("UNCHECKED_CAST")
        val observableContext = kryo.context[RpcObservableContextKey] as ObservableContext
        val observableId = RpcApi.ObservableId(input.readLong(true))
        val observable = UnicastSubject.create<Notification<Any>>()
        require(observableContext.observableMap.getIfPresent(observableId) == null) {
            "Multiple Observables arrived with the same ID $observableId"
        }
        observableContext.observableMap.put(observableId, observable)
        // We pin all Observables into a hard reference store (rooted in the RPC proxy) on subscription so that users
        // don't need to store a reference to the Observables themselves.
        // TODO Is this correct behaviour? It may result in unintended leaks in app code.
        return observable.pinInSubscriptions(observableContext.hardReferenceStore).doOnUnsubscribe {
            // This causes Future completions to give warnings because the corresponding OnComplete sent from the server
            // will arrive after the client unsubscribes from the observable and consequently invalidates the mapping.
            // The unsubscribe is due to [ObservableToFuture]'s use of first().
            observableContext.observableMap.invalidate(observableId)
        }.dematerialize()
    }

    override fun write(kryo: Kryo, output: Output, observable: Observable<Any>) {
        throw UnsupportedOperationException("Cannot serialise Observables on the client side")
    }
}