package net.corda.demobench.views

import de.jensd.fx.glyphs.fontawesome.FontAwesomeIcon
import de.jensd.fx.glyphs.fontawesome.FontAwesomeIconView
import de.jensd.fx.glyphs.fontawesome.utils.FontAwesomeIconFactory
import javafx.application.Platform
import javafx.geometry.Pos
import javafx.scene.control.ComboBox
import javafx.scene.control.SelectionMode.MULTIPLE
import javafx.scene.image.Image
import javafx.scene.image.ImageView
import javafx.scene.input.KeyCode
import javafx.scene.layout.Pane
import javafx.scene.layout.Priority
import javafx.stage.FileChooser
import javafx.util.StringConverter
import net.corda.core.node.CityDatabase
import net.corda.core.node.PhysicalLocation
import net.corda.core.utilities.DUMMY_NOTARY
import net.corda.demobench.model.*
import net.corda.demobench.ui.CloseableTab
import tornadofx.*
import java.nio.file.Path
import java.util.*

class NodeTabView : Fragment() {
    override val root = stackpane {}

    private val main by inject<DemoBenchView>()
    private val showConfig by param(true)

    private companion object : Component() {
        const val textWidth = 465.0
        const val maxNameLength = 15

        val jvm by inject<JVMConfig>()
    }

    private val nodeController by inject<NodeController>()
    private val serviceController by inject<ServiceController>()
    private val chooser = FileChooser()

    private val model = NodeDataModel()
    private val cordapps = LinkedList<Path>().observable()
    private val availableServices: List<String> = if (nodeController.hasNetworkMap()) serviceController.services else serviceController.notaries

    private val nodeTerminalView = find<NodeTerminalView>()
    private val nodeConfigView = stackpane {
        isVisible = showConfig
        styleClass += "config-view"

        form {
            fieldset("Configuration") {
                isFillWidth = false

                field("Legal name") { nodeNameField() }
                field("Nearest city") { nearestCityField() }
            }

            hbox {
                styleClass.addAll("node-panel")
                vboxConstraints { vGrow = Priority.ALWAYS }

                fieldset("CorDapps") {
                    hboxConstraints { hGrow = Priority.ALWAYS }
                    styleClass.addAll("cordapps-panel")

                    listview(cordapps) {
                        vboxConstraints { vGrow = Priority.ALWAYS }
                        setOnKeyPressed { key ->
                            if ((key.code == KeyCode.DELETE) && !selectionModel.isEmpty) {
                                cordapps.remove(selectionModel.selectedItem)
                            }
                            key.consume()
                        }
                    }
                }

                fieldset("Services") {
                    styleClass.addAll("services-panel")

                    listview(availableServices.observable()) {
                        vboxConstraints { vGrow = Priority.ALWAYS }
                        selectionModel.selectionMode = MULTIPLE
                        model.item.extraServices.set(selectionModel.selectedItems)
                    }
                }
            }

            hbox {
                button("Add CorDapp") {
                    setOnAction {
                        val app = (chooser.showOpenDialog(null) ?: return@setOnAction).toPath()
                        if (!cordapps.contains(app)) {
                            cordapps.add(app)
                        }
                    }

                    FontAwesomeIconFactory.get().setIcon(this, FontAwesomeIcon.PLUS)
                }

                // Spacer pane.
                pane {
                    hboxConstraints { hGrow = Priority.ALWAYS }
                }

                button("Start node") {
                    styleClass += "start-button"
                    setOnAction {
                        if (model.validate()) {
                            launch()
                            main.enableAddNodes()
                            main.enableSaveProfile()
                        }
                    }
                    graphic = FontAwesomeIconView(FontAwesomeIcon.PLAY_CIRCLE).apply { style += "-fx-fill: white" }
                }
            }
        }
    }

    val nodeTab = CloseableTab("New Node", root)

    init {
        // Ensure that we destroy the terminal along with the tab.
        nodeTab.setOnCloseRequest {
            nodeTerminalView.destroy()
        }
        nodeTab.graphic = FontAwesomeIconView(FontAwesomeIcon.BANK)

        root.add(nodeConfigView)
        root.add(nodeTerminalView)

        model.legalName.value = if (nodeController.hasNetworkMap()) "" else DUMMY_NOTARY.name.toString()
        model.nearestCity.value = if (nodeController.hasNetworkMap()) null else CityDatabase["London"]!!
        model.p2pPort.value = nodeController.nextPort
        model.rpcPort.value = nodeController.nextPort
        model.webPort.value = nodeController.nextPort
        model.h2Port.value = nodeController.nextPort

        chooser.title = "CorDapps"
        chooser.initialDirectory = jvm.dataHome.toFile()
        chooser.extensionFilters.add(FileChooser.ExtensionFilter("CorDapps (*.jar)", "*.jar", "*.JAR"))

        model.validate(focusFirstError = true)
    }

    private fun Pane.nodeNameField() = textfield(model.legalName) {
        minWidth = textWidth
        validator {
            if (it == null) {
                error("Node name is required")
            } else {
                val name = it.trim()
                if (name.isEmpty()) {
                    error("Node name is required")
                } else if (nodeController.nameExists(name)) {
                    error("Node with this name already exists")
                } else if (name.length > maxNameLength) {
                    error("Name is too long")
                } else {
                    null
                }
            }
        }
    }

    private val flags = runAsync {
        CityDatabase.cityMap.values.map { it.countryCode }.toSet().map { it to Image(resources["/net/corda/demobench/flags/$it.png"]) }.toMap()
    }

    private fun Pane.nearestCityField(): ComboBox<PhysicalLocation> {
        return combobox(model.nearestCity, CityDatabase.cityMap.values.toList().sortedBy { it.description }) {
            minWidth = textWidth
            styleClass += "city-picker"
            cellFormat {
                graphic = hbox(spacing = 10) {
                    imageview {
                        image = flags.get()[it.countryCode]
                    }
                    label(it.description)
                    alignment = Pos.CENTER_LEFT
                }
            }

            validator {
                if (it == null) error("Please select a city") else null
            }

            converter = object : StringConverter<PhysicalLocation>() {
                override fun toString(loc: PhysicalLocation?) = loc?.description ?: ""
                override fun fromString(string: String): PhysicalLocation? = CityDatabase[string]
            }

            value = CityDatabase["London"]

            isEditable = true
            makeAutocompletable()
        }
    }

    /**
     * Launches a Corda node that was configured via the form.
     */
    fun launch() {
        model.commit()
        val config = nodeController.validate(model.item)
        if (config != null) {
            nodeConfigView.isVisible = false
            config.install(cordapps)
            launchNode(config)
        }
    }

    /**
     * Launches a preconfigured Corda node, e.g. from a saved profile.
     */
    fun launch(config: NodeConfig) {
        nodeController.register(config)
        launchNode(config)
    }

    private fun launchNode(config: NodeConfig) {
        val countryCode = CityDatabase.cityMap[config.nearestCity]?.countryCode
        if (countryCode != null) {
            nodeTab.graphic = ImageView(flags.get()[countryCode]).apply { fitWidth = 24.0; isPreserveRatio = true }
        }
        nodeTab.text = config.legalName.toString()
        nodeTerminalView.open(config) { exitCode ->
            Platform.runLater {
                if (exitCode == 0)
                    nodeTab.requestClose()
                nodeController.dispose(config)
                main.forceAtLeastOneTab()
            }
        }

        nodeTab.setOnSelectionChanged {
            if (nodeTab.isSelected) {
                nodeTerminalView.takeFocus()
            }
        }
    }
}
