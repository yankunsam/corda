package net.corda.client.rpc

import com.google.common.base.Stopwatch
import com.google.common.net.HostAndPort
import com.google.common.util.concurrent.ListenableFuture
import net.corda.client.mock.Generator
import net.corda.client.mock.generateOrFail
import net.corda.client.mock.string
import net.corda.core.ThreadBox
import net.corda.core.div
import net.corda.core.future
import net.corda.core.messaging.RPCOps
import net.corda.core.utilities.ProcessUtilities
import net.corda.node.driver.*
import net.corda.node.services.RPCUserService
import net.corda.node.services.messaging.RpcServer
import net.corda.nodeapi.*
import org.apache.activemq.artemis.api.core.TransportConfiguration
import org.apache.activemq.artemis.api.core.client.ActiveMQClient
import org.apache.activemq.artemis.api.core.client.ActiveMQClient.DEFAULT_ACK_BATCH_SIZE
import org.apache.activemq.artemis.api.core.client.ClientSession
import org.apache.activemq.artemis.core.config.Configuration
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMAcceptorFactory
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory
import org.apache.activemq.artemis.core.security.CheckType
import org.apache.activemq.artemis.core.security.Role
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager3
import rx.Observable
import java.io.InputStream
import java.lang.reflect.Method
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.security.cert.X509Certificate
import kotlin.reflect.KCallable
import kotlin.reflect.jvm.reflect

data class RpcServerHandle(
        val hostAndPort: HostAndPort
)

val rpcTestUser = User("user1", "test", permissions = emptySet())
val fakeNodeLegalName = "not:a:valid:name"
interface RpcExposedDSLInterface : DriverDSLExposedInterface {
    // InVM
    fun <I : RPCOps> startInVmRpcServer(
            rpcUser: User = rpcTestUser,
            nodeLegalName: String = fakeNodeLegalName,
            ops : I
    ): ListenableFuture<Unit>

    fun <I : RPCOps> startInVmRpcClient(
            clazz: Class<I>,
            username: String = rpcTestUser.username,
            password: String = rpcTestUser.password
    ): ListenableFuture<I>

    fun startInVmArtemisSession(
            username: String = rpcTestUser.username,
            password: String = rpcTestUser.password
    ): ClientSession

    // Netty
    fun <I : RPCOps> startRpcServer(
            serverName: String = "driver-rpc-server",
            rpcUser: User = rpcTestUser,
            nodeLegalName: String = fakeNodeLegalName,
            ops : I
    ) : ListenableFuture<RpcServerHandle>

    fun <I : RPCOps> startRandomRpcClient(
            rpcOpsClass: Class<I>,
            rpcAddress: HostAndPort
    ): ListenableFuture<Process>

    fun <I : RPCOps> startRpcClient(
            clazz: Class<I>,
            rpcAddress: HostAndPort,
            username: String = rpcTestUser.username,
            password: String = rpcTestUser.password
    ): ListenableFuture<I>

    fun startArtemisSession(rpcAddress: HostAndPort): ClientSession
}
inline fun <reified I : RPCOps> RpcExposedDSLInterface.startInVmRpcClient(
        username: String = rpcTestUser.username,
        password: String = rpcTestUser.password
) = startInVmRpcClient(I::class.java, username, password)
inline fun <reified I : RPCOps> RpcExposedDSLInterface.startRandomRpcClient(hostAndPort: HostAndPort) =
        startRandomRpcClient(I::class.java, hostAndPort)
inline fun <reified I : RPCOps> RpcExposedDSLInterface.startRpcClient(
        rpcAddress: HostAndPort,
        username: String = rpcTestUser.username,
        password: String = rpcTestUser.password
) = startRpcClient(I::class.java, rpcAddress, username, password)

interface RpcInternalDSLInterface : DriverDSLInternalInterface, RpcExposedDSLInterface

fun <A> rpcDriver(
        isDebug: Boolean = false,
        driverDirectory: Path = Paths.get("build", getTimestampAsDirectoryName()),
        portAllocation: PortAllocation = PortAllocation.Incremental(10000),
        debugPortAllocation: PortAllocation = PortAllocation.Incremental(5005),
        systemProperties: Map<String, String> = emptyMap(),
        useTestClock: Boolean = false,
        automaticallyStartNetworkMap: Boolean = false,
        dsl: RpcExposedDSLInterface.() -> A
) = genericDriver(
        driverDsl = RpcDriverDSL(
                DriverDSL(
                        portAllocation = portAllocation,
                        debugPortAllocation = debugPortAllocation,
                        systemProperties = systemProperties,
                        driverDirectory = driverDirectory.toAbsolutePath(),
                        useTestClock = useTestClock,
                        automaticallyStartNetworkMap = automaticallyStartNetworkMap,
                        isDebug = isDebug
                )
        ),
        coerce = { it },
        dsl = dsl
)

private class SingleUserSecurityManager(val rpcUser: User) : ActiveMQSecurityManager3 {
    override fun validateUser(user: String?, password: String?) = isValid(user, password)
    override fun validateUserAndRole(user: String?, password: String?, roles: MutableSet<Role>?, checkType: CheckType?) = isValid(user, password)
    override fun validateUser(user: String?, password: String?, certificates: Array<out X509Certificate>?): String? {
        return validate(user, password)
    }
    override fun validateUserAndRole(user: String?, password: String?, roles: MutableSet<Role>?, checkType: CheckType?, address: String?, connection: RemotingConnection?): String? {
        return validate(user, password)
    }

    private fun isValid(user: String?, password: String?): Boolean {
        return rpcUser.username == user && rpcUser.password == password
    }
    private fun validate(user: String?, password: String?): String? {
        return if (isValid(user, password)) user else null
    }
}

data class RpcDriverDSL(
        val driverDSL: DriverDSL
) : DriverDSLInternalInterface by driverDSL, RpcInternalDSLInterface {
    private companion object {
        fun createInVmRpcServerArtemisConfig(): Configuration {
            return ConfigurationImpl().apply {
                acceptorConfigurations = setOf(TransportConfiguration(InVMAcceptorFactory::class.java.name))
                isPersistenceEnabled = false
                setPopulateValidatedUser(true)
            }
        }
        fun createRpcServerArtemisConfig(baseDirectory: Path, hostAndPort: HostAndPort): Configuration {
            val connectionDirection = ConnectionDirection.Inbound(acceptorFactoryClassName = NettyAcceptorFactory::class.java.name)
            return ConfigurationImpl().apply {
                val artemisDir = "$baseDirectory/artemis"
                bindingsDirectory = "$artemisDir/bindings"
                journalDirectory = "$artemisDir/journal"
                largeMessagesDirectory = "$artemisDir/large-messages"
                acceptorConfigurations = setOf(ArtemisTcpTransport.tcpTransport(connectionDirection, hostAndPort, null))
                setPopulateValidatedUser(true)
            }
        }
        val inVmClientTransportConfiguration = TransportConfiguration(InVMConnectorFactory::class.java.name)
        fun createNettyClientTransportConfiguration(hostAndPort: HostAndPort): TransportConfiguration {
            return ArtemisTcpTransport.tcpTransport(ConnectionDirection.Outbound(), hostAndPort, null)
        }
    }

    override fun <I : RPCOps> startInVmRpcServer(rpcUser: User, nodeLegalName: String, ops: I): ListenableFuture<Unit> {
        return driverDSL.executorService.submit<Unit> {
            val artemisConfig = createInVmRpcServerArtemisConfig()
            val server = EmbeddedActiveMQ()
            server.setConfiguration(artemisConfig)
            server.setSecurityManager(SingleUserSecurityManager(rpcUser))
            server.start()
            driverDSL.shutdownManager.registerShutdown {
                server.activeMQServer.stop()
                server.stop()
            }
            startRpcServerWithBrokerRunning(rpcUser, nodeLegalName, ops, inVmClientTransportConfiguration)
        }
    }

    override fun <I : RPCOps> startInVmRpcClient(clazz: Class<I>, username: String, password: String): ListenableFuture<I> {
        return driverDSL.executorService.submit<I> {
            val client = RpcClient<I>(inVmClientTransportConfiguration)
            val connection = client.start(clazz, username, password)
            driverDSL.shutdownManager.registerShutdown {
                connection.close()
            }
            connection.proxy
        }
    }

    override fun startInVmArtemisSession(username: String, password: String): ClientSession {
        val locator = ActiveMQClient.createServerLocatorWithoutHA(inVmClientTransportConfiguration)
        val sessionFactory = locator.createSessionFactory()
        val session = sessionFactory.createSession(username, password, false, true, true, locator.isPreAcknowledge, DEFAULT_ACK_BATCH_SIZE)
        driverDSL.shutdownManager.registerShutdown {
            session.close()
            sessionFactory.close()
            locator.close()
        }
        return session
    }

    override fun <I : RPCOps> startRpcServer(serverName: String, rpcUser: User, nodeLegalName: String, ops: I): ListenableFuture<RpcServerHandle> {
        val hostAndPort = driverDSL.portAllocation.nextHostAndPort()
        return driverDSL.executorService.submit<RpcServerHandle> {
            val artemisConfig = createRpcServerArtemisConfig(driverDSL.driverDirectory / serverName, hostAndPort)
            val server = ActiveMQServerImpl(artemisConfig, SingleUserSecurityManager(rpcUser))
            server.start()
            driverDSL.shutdownManager.registerShutdown {
                server.stop()
            }
            val transportConfiguration = createNettyClientTransportConfiguration(hostAndPort)
            startRpcServerWithBrokerRunning(rpcUser, nodeLegalName, ops, transportConfiguration)
            RpcServerHandle(hostAndPort)
        }
    }

    override fun <I : RPCOps> startRpcClient(clazz: Class<I>, rpcAddress: HostAndPort, username: String, password: String): ListenableFuture<I> {
        return driverDSL.executorService.submit<I> {
            val client = RpcClient<I>(ArtemisTcpTransport.tcpTransport(ConnectionDirection.Outbound(), rpcAddress, null))
            val connection = client.start(clazz, username, password)
            driverDSL.shutdownManager.registerShutdown {
                connection.close()
            }
            connection.proxy
        }
    }

    override fun <I : RPCOps> startRandomRpcClient(rpcOpsClass: Class<I>, rpcAddress: HostAndPort): ListenableFuture<Process> {
        val processFuture = driverDSL.executorService.submit<Process> {
            ProcessUtilities.startJavaProcess<RandomRpcUser>(listOf(rpcOpsClass.name, rpcAddress.toString()))
        }
        driverDSL.shutdownManager.registerProcessShutdown(processFuture)
        return processFuture
    }

    override fun startArtemisSession(rpcAddress: HostAndPort): ClientSession {

        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private fun <I : RPCOps> startRpcServerWithBrokerRunning(
            rpcUser: User,
            nodeLegalName: String,
            ops: I,
            transportConfiguration: TransportConfiguration
    ) {
        val locator = ActiveMQClient.createServerLocatorWithoutHA(transportConfiguration)
        val consumerSessionFactory = locator.createSessionFactory()
        val producerSessionFactory = locator.createSessionFactory()
        val consumerSession = consumerSessionFactory.createSession(rpcUser.username, rpcUser.password, false, true, true, locator.isPreAcknowledge, DEFAULT_ACK_BATCH_SIZE)
        val producerSession = producerSessionFactory.createSession(rpcUser.username, rpcUser.password, false, true, true, locator.isPreAcknowledge, DEFAULT_ACK_BATCH_SIZE)
        consumerSession.createTemporaryQueue(RpcApi.RPC_SERVER_QUEUE_NAME, RpcApi.RPC_SERVER_QUEUE_NAME)
        val consumerPool = (1..4).map { consumerSession.createConsumer(RpcApi.RPC_SERVER_QUEUE_NAME) }
        val producer = producerSession.createProducer()
        consumerSession.start()
        producerSession.start()
        val artemisState = ThreadBox(RpcServer.ArtemisState(producerSession, consumerPool, producer))
        val userService = object : RPCUserService {
            override fun getUser(username: String): User? = if (username == rpcUser.username) rpcUser else null
            override val users: List<User> get() = listOf(rpcUser)
        }
        val rpcServer = RpcServer(ops, driverDSL.executorService, artemisState, userService, nodeLegalName)
        driverDSL.shutdownManager.registerShutdown {
            rpcServer.close()
            consumerPool.forEach { it.close() }
            producer.close()
            consumerSessionFactory.close()
            producerSessionFactory.close()
            locator.close()
        }
        rpcServer.start()
    }
}

typealias MeasureParameter<T> = () -> Iterable<T>

fun <T> constant(value: T): MeasureParameter<T> = { listOf(value) }
fun range(intRange: IntRange) = { intRange }

data class MeasureResult<out R>(
    val parameters: List<Pair<String, Any?>>,
    val result: R
)

fun <A> iterateLexical(iterables: List<Iterable<A>>): Iterable<List<A>> {
    val result = ArrayList<List<A>>()
    fun iterateLexicalHelper(index: Int, list: List<A>) {
        if (index < iterables.size) {
            iterables[index].forEach {
                iterateLexicalHelper(index + 1, list + it)
            }
        } else {
            result.add(list)
        }
    }
    iterateLexicalHelper(0, emptyList())
    return result
}

private fun <R> measure(paramIterables: List<Iterable<Any?>>, kCallable: KCallable<R>, call: (Array<Any?>) -> R): Iterable<MeasureResult<R>> {
    val kParameters = kCallable.parameters
    return iterateLexical(paramIterables).map { params ->
        MeasureResult(
                parameters = params.mapIndexed { index, param -> Pair(kParameters[index].name!!, param) },
                result = call(params.toTypedArray())
        )
    }
}

fun <A, R> measure(a: Iterable<A>, f: (A) -> R) =
        measure(listOf(a), f.reflect()!!) { (f as ((Any?)->R))(it[0]) }
fun <A, B, R> measure(a: Iterable<A>, b: Iterable<B>, f: (A, B) -> R) =
        measure(listOf(a, b), f.reflect()!!) { (f as ((Any?,Any?)->R))(it[0], it[1]) }


interface Hello : RPCOps {
    fun echo(string: String): String
    fun delayedEcho(string: String): ListenableFuture<String>
    fun streamTime(): Observable<Instant>
    fun echoStream(stream: InputStream): InputStream
    fun echoStreams(stream1: InputStream, stream2: InputStream): InputStream
}

fun main(args: Array<String>) {
    rpcDriver(automaticallyStartNetworkMap = false) {

//        val handleFuture = startRpcServer("Alice", user, object : Hello {
        val handleFuture = startInVmRpcServer(ops = object : Hello {
            override fun echoStreams(stream1: InputStream, stream2: InputStream) = stream1

            override val protocolVersion = 0
            override fun streamTime() = Observable.interval(500, TimeUnit.MILLISECONDS).map { Instant.now() }
            override fun echo(string: String) = string
            override fun delayedEcho(string: String) = future { string }
            override fun echoStream(stream: InputStream): InputStream {
                println("echoStream called!")
//                val arr = stream.readBytes(1024 * 1024)
//                while (stream.read(arr) != -1) {}
                println("Read all the shit")
//                stream.close()
                return stream
//                return ByteArrayInputStream(arr)
//                stream.skip(1024 * 1024)
//                return future { Thread.sleep(2000) ; stream }
            }
        })
//
        val handle = handleFuture.get()
        val proxy = startInVmRpcClient<Hello>("test", "asd").get()
        proxy.delayedEcho("").get()
        proxy.echo("")
        //System.`in`.read()

        val returnStreamFuture = proxy.echoStream(RepeatingBytesInputStream(byteArrayOf(1, 2), 400 * 1024 * 1024))
        println("REPLY WOGHOO")
        val returnStream = returnStreamFuture
        val asd = ByteArray(4096)
        var allRead = 0
        while (true) {
            val read = returnStream.read(asd)
            if (read == -1) break
            allRead += read
        }
        println(allRead)
//        while (returnStream.read(asd) >= 0) { }
        return@rpcDriver

        fun asd() {
            val results = measure(1..10, listOf(1000)) { threadNumber, N ->
                val executor = Executors.newFixedThreadPool(threadNumber)
                val stopwatch = Stopwatch.createStarted()
                val latch = CountDownLatch(N)
                for (i in 1 .. N) {
                    executor.submit {
                        proxy.echo("$N")
                        latch.countDown()
                    }
                }
                latch.await()
                stopwatch.stop().elapsed(TimeUnit.MICROSECONDS)
            }

            results.forEach {
                println(it)
            }

        }

        asd()
        println("---")
        asd()
        println("---")
        asd()
//        val a = startRandomRpcClient<Hello>(handleFuture.get().hostAndPort).get()
//        Thread.sleep(50000)


//        val proxy = CordaRPCClient2.connect<Hello>(handleFuture.get().hostAndPort, "", "")
//        val asd = proxy.proxy.delayedEcho("hello")
//        asd.get()
//
//        val sub = proxy.proxy.streamTime().subscribe {
//            println("Hello $it")
//        }
//        Thread.sleep(2000)
//        //sub.unsubscribe()
//        proxy.stop()
//        for (i in 1 .. 1000) {
//            ByteArray(i * 10000)
//        }
//        Thread.sleep(10000)
////        println("Closing")
////        proxy.close()
////        Thread.sleep(5000)
////        (1..1000).toList().parallelStream().forEach {
////            println(hello.echo("$it"))
////        }
    }
}

class RandomRpcUser {

    companion object {
        private inline fun <reified T> HashMap<Class<*>, Generator<*>>.add(generator: Generator<T>) = this.putIfAbsent(T::class.java, generator)
        val generatorStore = HashMap<Class<*>, Generator<*>>().apply {
            add(Generator.string())
        }
        data class Call(val method: Method, val call: () -> Any?)

        @JvmStatic
        fun main(args: Array<String>) {
            require(args.size == 2)
            val rpcClass = Class.forName(args[0]) as Class<RPCOps>
            val hostAndPort = HostAndPort.fromString(args[1])
            val handle = RpcClient<RPCOps>(hostAndPort, null).start(rpcClass, "", "")
            val callGenerators = rpcClass.declaredMethods.map { method ->
                Generator.sequence(method.parameters.map {
                    generatorStore[it.type] ?: throw Exception("No generator for ${it.type}")
                }).map { arguments ->
                    Call(method, { method.invoke(handle.proxy, *arguments.toTypedArray()) })
                }
            }
            val callGenerator = Generator.choice(callGenerators)
            val random = SplittableRandom()

            val executor = Executors.newFixedThreadPool(4)

            log.info("Start")
            val count = AtomicInteger(0)
            for (i in 1 .. 1000) {
                val call = callGenerator.generateOrFail(random)
                //println("Calling ${call.method}")
                executor.submit {
//                    handle.proxy.echo("")
                    call.call()
                    log.info("${count.incrementAndGet()} done")
                }
            }
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)
            log.info("Done?")
        }
    }
}
