package net.corda.client.rpc

import com.google.common.net.HostAndPort
import net.corda.core.ThreadBox
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.RPCOps
import net.corda.core.random63BitValue
import net.corda.core.utilities.loggerFor
import net.corda.nodeapi.ArtemisTcpTransport.Companion.tcpTransport
import net.corda.nodeapi.ConnectionDirection
import net.corda.nodeapi.RpcApi
import net.corda.nodeapi.config.SSLConfiguration
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.api.core.TransportConfiguration
import org.apache.activemq.artemis.api.core.client.ActiveMQClient
import java.lang.reflect.Proxy

typealias CordaRPCClient2 = RpcClient<CordaRPCOps>
fun CordaRPCClient2.start(username: String, password: String) = start(CordaRPCOps::class.java, username, password)

data class RpcClientConfiguration(
        val threadPoolSize: Int, /** The size of the thread pool used for RPC (de)serialisation */
        val reapIntervalMs: Long /** The interval of observable reaping in milliseconds */
) {
    companion object {
        val default = RpcClientConfiguration(
                threadPoolSize = 2,
                reapIntervalMs = 1000
        )
    }
}

class RpcClient<I : RPCOps>(
        val transport: TransportConfiguration,
        val rpcConfiguration: RpcClientConfiguration = RpcClientConfiguration.default
) {
    constructor(
            hostAndPort: HostAndPort,
            sslConfiguration: SSLConfiguration? = null,
            rpcConfiguration: RpcClientConfiguration = RpcClientConfiguration.default
    ) : this(tcpTransport(ConnectionDirection.Outbound(), hostAndPort, sslConfiguration), rpcConfiguration)

    companion object {
        private val log = loggerFor<RpcClient<*>>()
    }

    interface RpcConnection<out I : RPCOps> {
        val proxy: I
        fun close()
    }

    fun start(rpcOpsClass: Class<I>, username: String, password: String): RpcConnection<I> {
        val clientAddress = SimpleString("${RpcApi.RPC_CLIENT_QUEUE_NAME_PREFIX}.${random63BitValue()}")
        val serverLocator = ActiveMQClient.createServerLocatorWithoutHA(transport)
        val sessionFactory = serverLocator.createSessionFactory()
        val session = sessionFactory.createSession(username, password, false, true, true, serverLocator.isPreAcknowledge, serverLocator.ackBatchSize)
        val producer = session.createProducer(RpcApi.RPC_SERVER_QUEUE_NAME)
        session.createQueue(clientAddress, clientAddress, false)
        val consumer = session.createConsumer(clientAddress)
        val artemisState = ThreadBox(RpcClientProxyHandler.ArtemisState(session, producer, consumer))
        val proxyHandler = RpcClientProxyHandler(rpcConfiguration, artemisState, clientAddress)
        proxyHandler.start()
        log.debug("RPC connected, returning proxy")
        @Suppress("UNCHECKED_CAST")
        val ops = Proxy.newProxyInstance(rpcOpsClass.classLoader, arrayOf(rpcOpsClass), proxyHandler) as I
        return object : RpcConnection<I> {
            override val proxy = ops
            override fun close() {
                proxyHandler.close()
                consumer.close()
                producer.close()
                session.close()
                sessionFactory.close()
                serverLocator.close()
            }
        }
    }
}
