package net.corda.core.node

import net.corda.core.messaging.CordaRPCOps
import net.corda.core.serialization.SerializationCustomization
import net.corda.core.utilities.loggerFor
import java.util.*
import java.util.function.Function

/**
 * Implement this on a class advertised in a META-INF/services/net.corda.core.node.CordaPluginRegistry file
 * to extend a Corda node with additional application services.
 */
abstract class CordaPluginRegistry {
    /**
     *
     */
    open val minimumPlatformVersion: Int get() = 1

    /**
     * List of lambdas returning JAX-RS objects. They may only depend on the RPC interface, as the webserver should
     * potentially be able to live in a process separate from the node itself.
     */
    open val webApis: List<Function<CordaRPCOps, out Any>> get() = emptyList()

    /**
     * Map of static serving endpoints to the matching resource directory. All endpoints will be prefixed with "/web" and
     * postfixed with "\*. Resource directories can be either on disk directories (especially when debugging) in the form
     * "a/b/c". Serving from a JAR can be specified with: javaClass.getResource("<folder-in-jar>").toExternalForm()
     */
    open val staticServeDirs: Map<String, String> get() = emptyMap()

    /**
     * A Map with an entry for each consumed Flow used by the webAPIs.
     * The key of each map entry should contain the FlowLogic<T> class name.
     * The associated map values are the union of all concrete class names passed to the Flow constructor.
     * Standard java.lang.* and kotlin.* types do not need to be included explicitly.
     * This is used to extend the white listed Flows that can be initiated from the ServiceHub invokeFlowAsync method.
     */
    open val requiredFlows: Map<String, Set<String>> get() = emptyMap()

    /**
     * List of lambdas constructing additional long lived services to be hosted within the node.
     * They expect a single [PluginServiceHub] parameter as input.
     * The [PluginServiceHub] will be fully constructed before the plugin service is created and will
     * allow access to the Flow factory and Flow initiation entry points there.
     */
    open val servicePlugins: List<Function<PluginServiceHub, out Any>> get() = emptyList()

    /**
     * Optionally whitelist types for use in object serialization, as we lock down the types that can be serialized.
     *
     * For example, if you add a new [net.corda.core.contracts.ContractState] it needs to be whitelisted.  You can do that
     * either by adding the @CordaSerializable annotation or via this method.
     *
     * @return true if you register types, otherwise you will be filtered out of the list of plugins considered in future.
     */
    open fun customizeSerialization(custom: SerializationCustomization): Boolean = false

    companion object {
        private val logger = loggerFor<CordaPluginRegistry>()

        /**
         * Fetch CordaPluginRegistry classes registered in META-INF/services/net.corda.core.node.CordaPluginRegistry
         * files that exist in the classpath
         */
        fun loadPlugins(platformVersion: Int): List<CordaPluginRegistry> {
            return ServiceLoader.load(CordaPluginRegistry::class.java).filter {
                if (platformVersion < it.minimumPlatformVersion) {
                    logger.warn("We're too old to load CorDapp ${it.javaClass.name} (its min platform version is ${it.minimumPlatformVersion})")
                    false
                } else {
                    true
                }
            }
        }
    }
}