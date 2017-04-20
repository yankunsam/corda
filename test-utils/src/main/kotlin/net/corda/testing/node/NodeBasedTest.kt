package net.corda.testing.node

import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import net.corda.core.createDirectories
import net.corda.core.crypto.X509Utilities
import net.corda.core.crypto.commonName
import net.corda.core.div
import net.corda.core.flatMap
import net.corda.core.map
import net.corda.core.node.services.ServiceInfo
import net.corda.core.node.services.ServiceType
import net.corda.core.utilities.DUMMY_MAP
import net.corda.node.internal.Node
import net.corda.node.services.config.ConfigHelper
import net.corda.node.services.config.FullNodeConfiguration
import net.corda.node.services.transactions.RaftValidatingNotaryService
import net.corda.node.utilities.ServiceIdentityGenerator
import net.corda.nodeapi.User
import net.corda.nodeapi.config.parseAs
import net.corda.testing.MOCK_NODE_VERSION_INFO
import net.corda.testing.getFreeLocalPorts
import org.apache.logging.log4j.Level
import org.bouncycastle.asn1.x500.X500Name
import org.junit.After
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import java.util.*
import kotlin.concurrent.thread

/**
 * Extend this class if you need to run nodes in a test. You could use the driver DSL but it's extremely slow for testing
 * purposes. Use the driver if you need to run the nodes in separate processes otherwise this class will suffice.
 */
// TODO Some of the logic here duplicates what's in the driver
abstract class NodeBasedTest {
    @Rule
    @JvmField
    val tempFolder = TemporaryFolder()

    private val nodes = ArrayList<Node>()
    private var _networkMapNode: Node? = null

    val networkMapNode: Node get() = _networkMapNode ?: startNetworkMapNode()

    init {
        System.setProperty("consoleLogLevel", Level.DEBUG.name().toLowerCase())
    }

    /**
     * Stops the network map node and all the nodes started by [startNode]. This is called automatically after each test
     * but can also be called manually within a test.
     */
    @After
    fun stopAllNodes() {
        nodes.forEach(Node::stop)
        nodes.clear()
        _networkMapNode = null
    }

    /**
     * You can use this method to start the network map node in a more customised manner. Otherwise it
     * will automatically be started with the default parameters.
     */
    fun startNetworkMapNode(legalName: X500Name = DUMMY_MAP.name,
                            advertisedServices: Set<ServiceInfo> = emptySet(),
                            rpcUsers: List<User> = emptyList(),
                            configOverrides: Map<String, Any> = emptyMap()): Node {
        check(_networkMapNode == null)
        return startNodeInternal(legalName, advertisedServices, rpcUsers, configOverrides).apply {
            _networkMapNode = this
        }
    }

    fun startNode(legalName: X500Name,
                  advertisedServices: Set<ServiceInfo> = emptySet(),
                  rpcUsers: List<User> = emptyList(),
                  configOverrides: Map<String, Any> = emptyMap()): ListenableFuture<Node> {
        val node = startNodeInternal(
                legalName,
                advertisedServices,
                rpcUsers,
                mapOf(
                        "networkMapService" to mapOf(
                                "address" to networkMapNode.configuration.p2pAddress.toString(),
                                "legalName" to networkMapNode.info.legalIdentity.name.toString()
                        )
                ) + configOverrides
        )
        return node.networkMapRegistrationFuture.map { node }
    }

    fun startNotaryCluster(notaryName: X500Name,
                           clusterSize: Int,
                           serviceType: ServiceType = RaftValidatingNotaryService.type): ListenableFuture<List<Node>> {
        ServiceIdentityGenerator.generateToDisk(
                (0 until clusterSize).map { tempFolder.root.toPath() / "$notaryName-$it" },
                serviceType.id,
                notaryName)

        val serviceInfo = ServiceInfo(serviceType, notaryName)
        val nodeAddresses = getFreeLocalPorts("localhost", clusterSize).map { it.toString() }

        val masterNodeFuture = startNode(
                X509Utilities.getDevX509Name("${notaryName.commonName}-0"),
                advertisedServices = setOf(serviceInfo),
                configOverrides = mapOf("notaryNodeAddress" to nodeAddresses[0]))

        val remainingNodesFutures = (1 until clusterSize).map {
            startNode(
                    X509Utilities.getDevX509Name("${notaryName.commonName}-$it"),
                    advertisedServices = setOf(serviceInfo),
                    configOverrides = mapOf(
                            "notaryNodeAddress" to nodeAddresses[it],
                            "notaryClusterAddresses" to listOf(nodeAddresses[0])))
        }

        return Futures.allAsList(remainingNodesFutures).flatMap { remainingNodes ->
            masterNodeFuture.map { masterNode -> listOf(masterNode) + remainingNodes }
        }
    }

    private fun startNodeInternal(legalName: X500Name,
                                  advertisedServices: Set<ServiceInfo>,
                                  rpcUsers: List<User>,
                                  configOverrides: Map<String, Any>): Node {
        val baseDirectory = (tempFolder.root.toPath() / legalName.commonName).createDirectories()
        val localPort = getFreeLocalPorts("localhost", 2)
        val config = ConfigHelper.loadConfig(
                baseDirectory = baseDirectory,
                allowMissingConfig = true,
                configOverrides = mapOf(
                        "myLegalName" to legalName.toString(),
                        "p2pAddress" to localPort[0].toString(),
                        "rpcAddress" to localPort[1].toString(),
                        "extraAdvertisedServiceIds" to advertisedServices.map { it.toString() },
                        "rpcUsers" to rpcUsers.map {
                            mapOf(
                                    "username" to it.username,
                                    "password" to it.password,
                                    "permissions" to it.permissions
                            )
                        }
                ) + configOverrides
        )

        val node = config.parseAs<FullNodeConfiguration>().createNode(MOCK_NODE_VERSION_INFO)
        node.start()
        nodes += node
        thread(name = legalName.commonName) {
            node.run()
        }
        return node
    }
}
