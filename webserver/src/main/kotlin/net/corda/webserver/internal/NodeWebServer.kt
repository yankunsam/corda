package net.corda.webserver.internal

import com.google.common.html.HtmlEscapers.htmlEscaper
import net.corda.client.rpc.CordaRPCClient
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.node.CordaPluginRegistry
import net.corda.core.utilities.loggerFor
import net.corda.nodeapi.ArtemisMessagingComponent
import net.corda.webserver.WebServerConfig
import net.corda.webserver.servlets.AttachmentDownloadServlet
import net.corda.webserver.servlets.DataUploadServlet
import net.corda.webserver.servlets.ObjectMapperConfig
import net.corda.webserver.servlets.ResponseFilter
import org.apache.activemq.artemis.api.core.ActiveMQNotConnectedException
import org.eclipse.jetty.server.*
import org.eclipse.jetty.server.handler.ErrorHandler
import org.eclipse.jetty.server.handler.HandlerCollection
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.eclipse.jetty.webapp.WebAppContext
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.server.ServerProperties
import org.glassfish.jersey.servlet.ServletContainer
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.Writer
import java.lang.reflect.InvocationTargetException
import java.util.ServiceLoader
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import javax.ws.rs.core.MediaType

class NodeWebServer(val config: WebServerConfig) {
    private companion object {
        val log = loggerFor<NodeWebServer>()
        const val retryDelay = 1000L // Milliseconds
    }

    val address = config.webAddress
    private var renderBasicInfoToConsole = true
    private lateinit var server: Server

    fun start() {
        logAndMaybePrint("Starting as webserver: ${config.webAddress}")
        server = initWebServer(retryConnectLocalRpc())
    }

    fun run() {
        while (server.isRunning) {
            Thread.sleep(100) // TODO: Redesign
        }
    }

    private fun initWebServer(localRpc: CordaRPCOps): Server {
        // Note that the web server handlers will all run concurrently, and not on the node thread.
        val handlerCollection = HandlerCollection()

        // Export JMX monitoring statistics and data over REST/JSON.
        if (config.exportJMXto.split(',').contains("http")) {
            val classpath = System.getProperty("java.class.path").split(System.getProperty("path.separator"))
            val warpath = classpath.firstOrNull { it.contains("jolokia-agent-war-2") && it.endsWith(".war") }
            if (warpath != null) {
                handlerCollection.addHandler(WebAppContext().apply {
                    // Find the jolokia WAR file on the classpath.
                    contextPath = "/monitoring/json"
                    setInitParameter("mimeType", MediaType.APPLICATION_JSON)
                    war = warpath
                })
            } else {
                log.warn("Unable to locate Jolokia WAR on classpath")
            }
        }

        // API, data upload and download to services (attachments, rates oracles etc)
        handlerCollection.addHandler(buildServletContextHandler(localRpc))

        val server = Server()

        val connector = if (config.useHTTPS) {
            val httpsConfiguration = HttpConfiguration()
            httpsConfiguration.outputBufferSize = 32768
            httpsConfiguration.addCustomizer(SecureRequestCustomizer())
            val sslContextFactory = SslContextFactory()
            sslContextFactory.keyStorePath = config.keyStoreFile.toString()
            sslContextFactory.setKeyStorePassword(config.keyStorePassword)
            sslContextFactory.setKeyManagerPassword(config.keyStorePassword)
            sslContextFactory.setTrustStorePath(config.trustStoreFile.toString())
            sslContextFactory.setTrustStorePassword(config.trustStorePassword)
            sslContextFactory.setExcludeProtocols("SSL.*", "TLSv1", "TLSv1.1")
            sslContextFactory.setIncludeProtocols("TLSv1.2")
            sslContextFactory.setExcludeCipherSuites(".*NULL.*", ".*RC4.*", ".*MD5.*", ".*DES.*", ".*DSS.*")
            sslContextFactory.setIncludeCipherSuites(".*AES.*GCM.*")
            val sslConnector = ServerConnector(server, SslConnectionFactory(sslContextFactory, "http/1.1"), HttpConnectionFactory(httpsConfiguration))
            sslConnector.port = address.port
            sslConnector
        } else {
            val httpConfiguration = HttpConfiguration()
            httpConfiguration.outputBufferSize = 32768
            val httpConnector = ServerConnector(server, HttpConnectionFactory(httpConfiguration))
            httpConnector.port = address.port
            httpConnector
        }
        server.connectors = arrayOf<Connector>(connector)

        server.handler = handlerCollection
        server.start()
        log.info("Starting webserver on address $address")
        return server
    }

    private fun buildServletContextHandler(localRpc: CordaRPCOps): ServletContextHandler {
        val safeLegalName = htmlEscaper().escape(config.myLegalName)
        return ServletContextHandler().apply {
            contextPath = "/"
            errorHandler = object : ErrorHandler() {
                @Throws(IOException::class)
                override fun writeErrorPageHead(request: HttpServletRequest, writer: Writer, code: Int, message: String) {
                    writer.write("<meta http-equiv=\"Content-Type\" content=\"text/html;charset=utf-8\"/>\n")
                    writer.write("<title>Corda $safeLegalName : Error $code</title>\n")
                }

                @Throws(IOException::class)
                override fun writeErrorPageMessage(request: HttpServletRequest, writer: Writer, code: Int, message: String , uri: String) {
                    writer.write("<h1>Corda $safeLegalName</h1>\n")
                    super.writeErrorPageMessage(request, writer, code, message, uri)
                }
            }
            setAttribute("rpc", localRpc)
            addServlet(DataUploadServlet::class.java, "/upload/*")
            addServlet(AttachmentDownloadServlet::class.java, "/attachments/*")

            val resourceConfig = ResourceConfig()
                .register(ObjectMapperConfig(localRpc))
                .register(ResponseFilter())
                .register(APIServerImpl(localRpc))

            val webAPIsOnClasspath = pluginRegistries.flatMap { x -> x.webApis }
            for (webapi in webAPIsOnClasspath) {
                log.info("Add plugin web API from attachment $webapi")
                val customAPI = try {
                    webapi.apply(localRpc)
                } catch (ex: InvocationTargetException) {
                    log.error("Constructor $webapi threw an error: ", ex.targetException)
                    continue
                }
                resourceConfig.register(customAPI)
            }

            val staticDirMaps = pluginRegistries.map { x -> x.staticServeDirs }
            val staticDirs = staticDirMaps.flatMap { it.keys }.zip(staticDirMaps.flatMap { it.values })
            staticDirs.forEach {
                val staticDir = ServletHolder(DefaultServlet::class.java)
                staticDir.setInitParameter("resourceBase", it.second)
                staticDir.setInitParameter("dirAllowed", "true")
                staticDir.setInitParameter("pathInfoOnly", "true")
                addServlet(staticDir, "/web/${it.first}/*")
            }

            addServlet(ServletHolder(HelpServlet("/web/${staticDirs.first().first}")), "/")

            // Give the app a slightly better name in JMX rather than a randomly generated one and enable JMX
            resourceConfig.addProperties(mapOf(ServerProperties.APPLICATION_NAME to "node.api",
                    ServerProperties.MONITORING_STATISTICS_MBEANS_ENABLED to "true"))

            val container = ServletContainer(resourceConfig)
            val jerseyServlet = ServletHolder(container)
            addServlet(jerseyServlet, "/api/*")
            jerseyServlet.initOrder = 0 // Initialise at server start
        }
    }

    private inner class HelpServlet(private val redirectTo: String) : HttpServlet() {
        override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {
            resp.sendRedirect(resp.encodeRedirectURL(redirectTo))
        }
    }

    private fun retryConnectLocalRpc(): CordaRPCOps {
        while (true) {
            try {
                return connectLocalRpcAsNodeUser()
            } catch (e: ActiveMQNotConnectedException) {
                log.debug("Could not connect to ${config.p2pAddress} due to exception: ", e)
                Thread.sleep(retryDelay)
                // This error will happen if the server has yet to create the keystore
                // Keep the fully qualified package name due to collisions with the Kotlin stdlib
                // exception of the same name
            } catch (e: java.nio.file.NoSuchFileException) {
                log.debug("Tried to open a file that doesn't yet exist, retrying", e)
                Thread.sleep(retryDelay)
            } catch (e: Throwable) {
                // E.g. a plugin cannot be instantiated?
                // Note that we do want the exception stacktrace.
                log.error("Cannot start WebServer", e)
                throw e
            }
        }
    }

    private fun connectLocalRpcAsNodeUser(): CordaRPCOps {
        log.info("Connecting to node at ${config.p2pAddress} as node user")
        val client = CordaRPCClient(config.p2pAddress, config)
        client.start(ArtemisMessagingComponent.NODE_USER, ArtemisMessagingComponent.NODE_USER)
        return client.proxy()
    }

    /** Fetch CordaPluginRegistry classes registered in META-INF/services/net.corda.core.node.CordaPluginRegistry files that exist in the classpath */
    val pluginRegistries: List<CordaPluginRegistry> by lazy {
        ServiceLoader.load(CordaPluginRegistry::class.java).toList()
    }

    /** Used for useful info that we always want to show, even when not logging to the console */
    fun logAndMaybePrint(description: String, info: String? = null) {
        val msg = if (info == null) description else "${description.padEnd(40)}: $info"
        val loggerName = if (renderBasicInfoToConsole) "BasicInfo" else "Main"
        LoggerFactory.getLogger(loggerName).info(msg)
    }
}
