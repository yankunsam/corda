package net.corda.verifier

import com.google.common.net.HostAndPort
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.ListeningScheduledExecutorService
import com.google.common.util.concurrent.SettableFuture
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import net.corda.core.crypto.X509Utilities
import net.corda.core.crypto.commonName
import net.corda.core.div
import net.corda.core.map
import net.corda.core.random63BitValue
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.utilities.ProcessUtilities
import net.corda.core.utilities.loggerFor
import net.corda.node.driver.*
import net.corda.node.services.config.configureDevKeyAndTrustStores
import net.corda.nodeapi.ArtemisMessagingComponent.Companion.NODE_USER
import net.corda.nodeapi.ArtemisTcpTransport
import net.corda.nodeapi.ConnectionDirection
import net.corda.nodeapi.VerifierApi
import net.corda.nodeapi.config.SSLConfiguration
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.api.core.client.ActiveMQClient
import org.apache.activemq.artemis.api.core.client.ClientProducer
import org.apache.activemq.artemis.api.core.client.ClientSession
import org.apache.activemq.artemis.core.config.Configuration
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory
import org.apache.activemq.artemis.core.security.CheckType
import org.apache.activemq.artemis.core.security.Role
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager
import org.bouncycastle.asn1.x500.X500Name
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * This file defines an extension to [DriverDSL] that allows starting of verifier processes and
 * lightweight verification requestors.
 */
interface VerifierExposedDSLInterface : DriverDSLExposedInterface {
    /** Starts a lightweight verification requestor that implements the Node's Verifier API */
    fun startVerificationRequestor(name: X500Name): ListenableFuture<VerificationRequestorHandle>

    /** Starts an out of process verifier connected to [address] */
    fun startVerifier(address: HostAndPort): ListenableFuture<VerifierHandle>

    /**
     * Waits until [number] verifiers are listening for verification requests coming from the Node. Check
     * [VerificationRequestorHandle.waitUntilNumberOfVerifiers] for an equivalent for requestors.
     */
    fun NodeHandle.waitUntilNumberOfVerifiers(number: Int)
}

/** Starts a verifier connecting to the specified node */
fun VerifierExposedDSLInterface.startVerifier(nodeHandle: NodeHandle) =
        startVerifier(nodeHandle.configuration.p2pAddress)

/** Starts a verifier connecting to the specified requestor */
fun VerifierExposedDSLInterface.startVerifier(verificationRequestorHandle: VerificationRequestorHandle) =
        startVerifier(verificationRequestorHandle.p2pAddress)

interface VerifierInternalDSLInterface : DriverDSLInternalInterface, VerifierExposedDSLInterface

/**
 * Behaves the same as [driver] and adds verifier-related functionality.
 */
fun <A> verifierDriver(
        isDebug: Boolean = false,
        driverDirectory: Path = Paths.get("build", getTimestampAsDirectoryName()),
        portAllocation: PortAllocation = PortAllocation.Incremental(10000),
        debugPortAllocation: PortAllocation = PortAllocation.Incremental(5005),
        systemProperties: Map<String, String> = emptyMap(),
        useTestClock: Boolean = false,
        automaticallyStartNetworkMap: Boolean = true,
        dsl: VerifierExposedDSLInterface.() -> A
) = genericDriver(
        driverDsl = VerifierDriverDSL(
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

/** A handle for a verifier */
data class VerifierHandle(
        val process: Process
)

/** A handle for the verification requestor */
data class VerificationRequestorHandle(
        val p2pAddress: HostAndPort,
        private val responseAddress: SimpleString,
        private val session: ClientSession,
        private val requestProducer: ClientProducer,
        private val addVerificationFuture: (Long, SettableFuture<Throwable?>) -> Unit,
        private val executorService: ListeningScheduledExecutorService
) {
    fun verifyTransaction(transaction: LedgerTransaction): ListenableFuture<Throwable?> {
        val message = session.createMessage(false)
        val verificationId = random63BitValue()
        val request = VerifierApi.VerificationRequest(verificationId, transaction, responseAddress)
        request.writeToClientMessage(message)
        val verificationFuture = SettableFuture.create<Throwable?>()
        addVerificationFuture(verificationId, verificationFuture)
        requestProducer.send(message)
        return verificationFuture
    }

    fun waitUntilNumberOfVerifiers(number: Int) {
        poll(executorService, "$number verifiers to come online") {
            if (session.queueQuery(SimpleString(VerifierApi.VERIFICATION_REQUESTS_QUEUE_NAME)).consumerCount >= number) {
                Unit
            } else {
                null
            }
        }.get()
    }
}


data class VerifierDriverDSL(
        val driverDSL: DriverDSL
) : DriverDSLInternalInterface by driverDSL, VerifierInternalDSLInterface {
    val verifierCount = AtomicInteger(0)

    companion object {
        private val log = loggerFor<VerifierDriverDSL>()
        fun createConfiguration(baseDirectory: Path, nodeHostAndPort: HostAndPort): Config {
            return ConfigFactory.parseMap(
                    mapOf(
                            "baseDirectory" to baseDirectory.toString(),
                            "nodeHostAndPort" to nodeHostAndPort.toString()
                    )
            )
        }

        fun createVerificationRequestorArtemisConfig(baseDirectory: Path, responseAddress: String, hostAndPort: HostAndPort, sslConfiguration: SSLConfiguration): Configuration {
            val connectionDirection = ConnectionDirection.Inbound(acceptorFactoryClassName = NettyAcceptorFactory::class.java.name)
            return ConfigurationImpl().apply {
                val artemisDir = "$baseDirectory/artemis"
                bindingsDirectory = "$artemisDir/bindings"
                journalDirectory = "$artemisDir/journal"
                largeMessagesDirectory = "$artemisDir/large-messages"
                acceptorConfigurations = setOf(ArtemisTcpTransport.tcpTransport(connectionDirection, hostAndPort, sslConfiguration))
                queueConfigurations = listOf(
                        CoreQueueConfiguration().apply {
                            name = VerifierApi.VERIFICATION_REQUESTS_QUEUE_NAME
                            address = VerifierApi.VERIFICATION_REQUESTS_QUEUE_NAME
                            isDurable = false
                        },
                        CoreQueueConfiguration().apply {
                            name = responseAddress
                            address = responseAddress
                            isDurable = false
                        }
                )
            }
        }
    }

    override fun startVerificationRequestor(name: X500Name): ListenableFuture<VerificationRequestorHandle> {
        val hostAndPort = driverDSL.portAllocation.nextHostAndPort()
        return driverDSL.executorService.submit<VerificationRequestorHandle> {
            startVerificationRequestorInternal(name, hostAndPort)
        }
    }

    private fun startVerificationRequestorInternal(name: X500Name, hostAndPort: HostAndPort): VerificationRequestorHandle {
        val baseDir = driverDSL.driverDirectory / name.commonName
        val sslConfig = object : SSLConfiguration {
            override val certificatesDirectory = baseDir / "certificates"
            override val keyStorePassword: String get() = "cordacadevpass"
            override val trustStorePassword: String get() = "trustpass"
        }
        sslConfig.configureDevKeyAndTrustStores(name)

        val responseQueueNonce = random63BitValue()
        val responseAddress = "${VerifierApi.VERIFICATION_RESPONSES_QUEUE_NAME_PREFIX}.$responseQueueNonce"

        val artemisConfig = createVerificationRequestorArtemisConfig(baseDir, responseAddress, hostAndPort, sslConfig)

        val securityManager = object : ActiveMQSecurityManager {
            // We don't need auth, SSL is good enough
            override fun validateUser(user: String?, password: String?) = true

            override fun validateUserAndRole(user: String?, password: String?, roles: MutableSet<Role>?, checkType: CheckType?) = true
        }

        val server = ActiveMQServerImpl(artemisConfig, securityManager)
        log.info("Starting verification requestor Artemis server with base dir $baseDir")
        server.start()
        driverDSL.shutdownManager.registerShutdown(Futures.immediateFuture {
            server.stop()
        })

        val locator = ActiveMQClient.createServerLocatorWithoutHA()
        val transport = ArtemisTcpTransport.tcpTransport(ConnectionDirection.Outbound(), hostAndPort, sslConfig)
        val sessionFactory = locator.createSessionFactory(transport)
        val session = sessionFactory.createSession()
        driverDSL.shutdownManager.registerShutdown(Futures.immediateFuture {
            session.stop()
            sessionFactory.close()
        })
        val producer = session.createProducer(VerifierApi.VERIFICATION_REQUESTS_QUEUE_NAME)

        val consumer = session.createConsumer(responseAddress)
        // We demux the individual txs ourselves to avoid race when a new verifier is added
        val verificationResponseFutures = ConcurrentHashMap<Long, SettableFuture<Throwable?>>()
        consumer.setMessageHandler {
            val result = VerifierApi.VerificationResponse.fromClientMessage(it)
            val resultFuture = verificationResponseFutures.remove(result.verificationId)
            log.info("${verificationResponseFutures.size} verifications left")
            if (resultFuture != null) {
                resultFuture.set(result.exception)
            } else {
                log.warn("Verification requestor $name can't find tx result future with id ${result.verificationId}, possible dupe")
            }
        }
        session.start()
        return VerificationRequestorHandle(
                p2pAddress = hostAndPort,
                responseAddress = SimpleString(responseAddress),
                session = session,
                requestProducer = producer,
                addVerificationFuture = { verificationNonce, future ->
                    verificationResponseFutures.put(verificationNonce, future)
                },
                executorService = driverDSL.executorService
        )
    }

    override fun startVerifier(address: HostAndPort): ListenableFuture<VerifierHandle> {
        log.info("Starting verifier connecting to address $address")
        val id = verifierCount.andIncrement
        val jdwpPort = if (driverDSL.isDebug) driverDSL.debugPortAllocation.nextPort() else null
        val processFuture = driverDSL.executorService.submit<Process> {
            val verifierName = X509Utilities.getDevX509Name("verifier$id")
            val baseDirectory = driverDSL.driverDirectory / verifierName.commonName
            val config = createConfiguration(baseDirectory, address)
            val configFilename = "verifier.conf"
            writeConfig(baseDirectory, configFilename, config)
            Verifier.loadConfiguration(baseDirectory, baseDirectory / configFilename).configureDevKeyAndTrustStores(verifierName)
            ProcessUtilities.startJavaProcess<Verifier>(listOf(baseDirectory.toString()), jdwpPort = jdwpPort)
        }
        driverDSL.shutdownManager.registerProcessShutdown(processFuture)
        return processFuture.map(::VerifierHandle)
    }

    private fun <A> NodeHandle.connectToNode(closure: (ClientSession) -> A): A {
        val transport = ArtemisTcpTransport.tcpTransport(ConnectionDirection.Outbound(), configuration.p2pAddress, configuration)
        val locator = ActiveMQClient.createServerLocatorWithoutHA(transport)
        val sessionFactory = locator.createSessionFactory()
        val session = sessionFactory.createSession(NODE_USER, NODE_USER, false, true, true, locator.isPreAcknowledge, locator.ackBatchSize)
        return session.use {
            closure(it)
        }
    }

    override fun NodeHandle.waitUntilNumberOfVerifiers(number: Int) {
        connectToNode { session ->
            poll(driverDSL.executorService, "$number verifiers to come online") {
                if (session.queueQuery(SimpleString(VerifierApi.VERIFICATION_REQUESTS_QUEUE_NAME)).consumerCount >= number) {
                    Unit
                } else {
                    null
                }
            }.get()
        }
    }
}
