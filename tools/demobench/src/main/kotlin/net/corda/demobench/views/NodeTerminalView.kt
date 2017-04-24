package net.corda.demobench.views

import com.jediterm.terminal.TerminalColor
import com.jediterm.terminal.TextStyle
import com.jediterm.terminal.ui.settings.DefaultSettingsProvider
import javafx.application.Platform
import javafx.embed.swing.SwingNode
import javafx.scene.control.Button
import javafx.scene.control.Label
import javafx.scene.image.ImageView
import javafx.scene.layout.StackPane
import javafx.scene.layout.VBox
import javafx.util.Duration
import net.corda.client.rpc.notUsed
import net.corda.core.success
import net.corda.core.then
import net.corda.demobench.explorer.ExplorerController
import net.corda.demobench.model.NodeConfig
import net.corda.demobench.model.NodeController
import net.corda.demobench.model.NodeState
import net.corda.demobench.pty.R3Pty
import net.corda.demobench.rpc.NodeRPC
import net.corda.demobench.ui.PropertyLabel
import net.corda.demobench.web.DBViewer
import net.corda.demobench.web.WebServerController
import tornadofx.*
import java.awt.Dimension
import java.net.URI
import java.util.logging.Level
import javax.swing.SwingUtilities

class NodeTerminalView : Fragment() {
    override val root by fxml<VBox>()

    private val nodeController by inject<NodeController>()
    private val explorerController by inject<ExplorerController>()
    private val webServerController by inject<WebServerController>()

    private val nodeName by fxid<Label>()
    private val states by fxid<PropertyLabel>()
    private val transactions by fxid<PropertyLabel>()
    private val balance by fxid<PropertyLabel>()

    private val viewDatabaseButton by fxid<Button>()
    private val launchWebButton by fxid<Button>()
    private val launchExplorerButton by fxid<Button>()

    private var isDestroyed: Boolean = false
    private val explorer = explorerController.explorer()
    private val webServer = webServerController.webServer()
    private val viewer = DBViewer()
    private var rpc: NodeRPC? = null
    private var pty: R3Pty? = null
    private lateinit var logo: ImageView
    private lateinit var swingTerminal: SwingNode

    fun open(config: NodeConfig, onExit: (Int) -> Unit) {
        nodeName.text = config.legalName.toString()

        swingTerminal = SwingNode()
        swingTerminal.setOnMouseClicked {
            swingTerminal.requestFocus()
        }

        logo = ImageView(resources["corda-logo-square-trans.png"])
        logo.opacity = 0.0
        swingTerminal.styleClass += "terminal-widget"
        val stack = StackPane(logo, swingTerminal)
        root.children.add(stack)
        root.isVisible = true

        SwingUtilities.invokeLater({
            val r3pty = R3Pty(config.legalName, TerminalSettingsProvider(), Dimension(160, 80), onExit)
            pty = r3pty

            if (nodeController.runCorda(r3pty, config)) {
                swingTerminal.content = r3pty.terminal

                configureDatabaseButton(config)
                configureExplorerButton(config)
                configureWebButton(config)

                /*
                 * Start RPC client that will update node statistics on UI.
                 */
                rpc = launchRPC(config)

                /*
                 * Check whether the PTY has exited unexpectedly,
                 * and close the RPC client if it has.
                 */
                if (!r3pty.isConnected) {
                    log.severe("Node '${config.legalName}' has failed to start.")
                    swingTerminal.content = null
                    rpc?.close()
                }
            }
        })
    }

    fun enable(config: NodeConfig) {
        config.state = NodeState.RUNNING
        log.info("Node '${config.legalName}' is now ready.")

        launchExplorerButton.isDisable = false
        viewDatabaseButton.isDisable = false
        launchWebButton.isDisable = false
    }

    /*
     * We only want to run one explorer for each node.
     * So disable the "launch" button when we have
     * launched the explorer and only reenable it when
     * the explorer has exited.
     */
    fun configureExplorerButton(config: NodeConfig) {
        launchExplorerButton.setOnAction {
            launchExplorerButton.isDisable = true

            explorer.open(config, onExit = {
                launchExplorerButton.isDisable = false
            })
        }
    }

    fun configureDatabaseButton(config: NodeConfig) {
        viewDatabaseButton.setOnAction {
            viewer.openBrowser(config.h2Port)
        }
    }

    private var webURL: URI? = null

    /*
     * We only want to run one web server for each node.
     * So disable the "launch" button when we have
     * launched the web server and only reenable it when
     * the web server has exited.
     */
    fun configureWebButton(config: NodeConfig) {
        launchWebButton.setOnAction {
            if (webURL != null) {
                app.hostServices.showDocument(webURL.toString())
                return@setOnAction
            }
            launchWebButton.isDisable = true

            log.info("Starting web server for ${config.legalName}")
            webServer.open(config) then {
                Platform.runLater { launchWebButton.isDisable = false }
            } success {
                log.info("Web server for ${config.legalName} started on $it")
                Platform.runLater {
                    webURL = it
                    app.hostServices.showDocument(it.toString())
                }
            }
        }
    }

    fun launchRPC(config: NodeConfig) = NodeRPC(config, start = { enable(config) }, invoke = { ops ->
        try {
            val verifiedTx = ops.verifiedTransactions()
            val statesInVault = ops.vaultAndUpdates()
            val cashBalances = ops.getCashBalances().entries.joinToString(
                    separator = ", ",
                    transform = { e -> e.value.toString() }
            )

            Platform.runLater {
                logo.opacityProperty().animate(1.0, Duration.seconds(2.5))
                states.value = fetchAndDrop(statesInVault).size.toString()
                transactions.value = fetchAndDrop(verifiedTx).size.toString()
                balance.value = if (cashBalances.isNullOrEmpty()) "0" else cashBalances
            }
        } catch (e: Exception) {
            log.log(Level.WARNING, "RPC failed: ${e.message}", e)
        }
    })

    fun destroy() {
        if (!isDestroyed) {
            webServer.close()
            explorer.close()
            viewer.close()
            rpc?.close()
            pty?.close()
            isDestroyed = true
        }
    }

    fun takeFocus() {
        // Request focus. This also forces a repaint, without this, for some reason the terminal has gone to sleep
        // and is no longer updating in response to new data from the pty.
        Platform.runLater {
            swingTerminal.requestFocus()
        }
    }

    // TODO - Will change when we modify RPC Observables handling.
    private fun <T> fetchAndDrop(pair: Pair<T, rx.Observable<*>>): T {
        pair.second.notUsed()
        return pair.first
    }

    class TerminalSettingsProvider : DefaultSettingsProvider() {
        override fun getDefaultStyle() = TextStyle(TerminalColor.WHITE, TerminalColor.rgb(50, 50, 50))
        override fun emulateX11CopyPaste() = true
    }
}
