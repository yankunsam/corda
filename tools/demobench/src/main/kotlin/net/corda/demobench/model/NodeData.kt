package net.corda.demobench.model

import javafx.beans.property.SimpleIntegerProperty
import javafx.beans.property.SimpleListProperty
import javafx.beans.property.SimpleObjectProperty
import javafx.beans.property.SimpleStringProperty
import net.corda.core.node.CityDatabase
import tornadofx.*

object SuggestedDetails {
    val banks = listOf(
            // Mike:  Rome? Why Rome?
            // Roger: Notaries public (also called "notaries", "notarial officers", or "public notaries") hold an office
            //        which can trace its origins back to the ancient Roman Republic, when they were called scribae ("scribes"),
            //        tabelliones forenses, or personae publicae.[4]
            // Mike:  Can't argue with that. It's even got a citation.
            "Notary" to "Rome",
            "Bank of Breakfast Tea" to "Liverpool",
            "Bank of Big Apples" to "New York",
            "Bank of Baguettes" to "Paris",
            "Bank of Fondue" to "Geneve",
            "Bank of Maple Syrup" to "Toronto"
    )

    private var cursor = 0

    val nextBank: Pair<String, String> get() = banks[cursor++ % banks.size]
}

class NodeData {
    val legalName = SimpleStringProperty("")
    val nearestCity = SimpleObjectProperty(CityDatabase["London"]!!)
    val p2pPort = SimpleIntegerProperty()
    val rpcPort = SimpleIntegerProperty()
    val webPort = SimpleIntegerProperty()
    val h2Port = SimpleIntegerProperty()
    val extraServices = SimpleListProperty(mutableListOf<String>().observable())
}

class NodeDataModel : ItemViewModel<NodeData>(NodeData()) {
    val legalName = bind { item?.legalName }
    val nearestCity = bind { item?.nearestCity }
    val p2pPort = bind { item?.p2pPort }
    val rpcPort = bind { item?.rpcPort }
    val webPort = bind { item?.webPort }
    val h2Port = bind { item?.h2Port }
}
