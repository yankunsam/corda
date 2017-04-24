package net.corda.explorer.views

import de.jensd.fx.glyphs.fontawesome.FontAwesomeIcon
import javafx.beans.binding.Bindings
import javafx.beans.value.ObservableValue
import javafx.collections.ObservableList
import javafx.geometry.HPos
import javafx.geometry.Insets
import javafx.geometry.Pos
import javafx.scene.Node
import javafx.scene.Parent
import javafx.scene.control.Label
import javafx.scene.control.ListView
import javafx.scene.control.TableView
import javafx.scene.control.TitledPane
import javafx.scene.layout.BorderPane
import javafx.scene.layout.VBox
import net.corda.client.jfx.model.*
import net.corda.client.jfx.utils.filterNotNull
import net.corda.client.jfx.utils.lift
import net.corda.client.jfx.utils.map
import net.corda.client.jfx.utils.sequence
import net.corda.contracts.asset.Cash
import net.corda.core.contracts.*
import net.corda.core.crypto.*
import net.corda.core.node.NodeInfo
import net.corda.explorer.AmountDiff
import net.corda.explorer.formatters.AmountFormatter
import net.corda.explorer.identicon.identicon
import net.corda.explorer.identicon.identiconToolTip
import net.corda.explorer.model.CordaView
import net.corda.explorer.model.CordaWidget
import net.corda.explorer.model.ReportingCurrencyModel
import net.corda.explorer.sign
import net.corda.explorer.ui.setCustomCellFactory
import tornadofx.*
import java.util.*

class TransactionViewer : CordaView("Transactions") {
    override val root by fxml<BorderPane>()
    override val icon = FontAwesomeIcon.EXCHANGE

    private val transactionViewTable by fxid<TableView<Transaction>>()
    private val matchingTransactionsLabel by fxid<Label>()
    // Inject data
    private val transactions by observableListReadOnly(TransactionDataModel::partiallyResolvedTransactions)
    private val reportingExchange by observableValue(ReportingCurrencyModel::reportingExchange)
    private val reportingCurrency by observableValue(ReportingCurrencyModel::reportingCurrency)
    private val myIdentity by observableValue(NetworkIdentityModel::myIdentity)

    override val widgets = listOf(CordaWidget(title, TransactionWidget())).observable()

    /**
     * This is what holds data for a single transaction node. Note how a lot of these are nullable as we often simply don't
     * have the data.
     */
    data class Transaction(
            val tx: PartiallyResolvedTransaction,
            val id: SecureHash,
            val inputs: Inputs,
            val outputs: ObservableList<StateAndRef<ContractState>>,
            val inputParties: ObservableList<List<ObservableValue<NodeInfo?>>>,
            val outputParties: ObservableList<List<ObservableValue<NodeInfo?>>>,
            val commandTypes: List<Class<CommandData>>,
            val totalValueEquiv: ObservableValue<AmountDiff<Currency>>
    )

    data class Inputs(val resolved: ObservableList<StateAndRef<ContractState>>, val unresolved: ObservableList<StateRef>)

    /**
     * We map the gathered data about transactions almost one-to-one to the nodes.
     */
    init {
        val transactions = transactions.map {
            val resolved = it.inputs.sequence()
                    .map { it as? PartiallyResolvedTransaction.InputResolution.Resolved }
                    .filterNotNull()
                    .map { it.stateAndRef }
            val unresolved = it.inputs.sequence()
                    .map { it as? PartiallyResolvedTransaction.InputResolution.Unresolved }
                    .filterNotNull()
                    .map { it.stateRef }
            val outputs = it.transaction.tx.outputs
                    .mapIndexed { index, transactionState ->
                        val stateRef = StateRef(it.id, index)
                        StateAndRef(transactionState, stateRef)
                    }.observable()
            Transaction(
                    tx = it,
                    id = it.id,
                    inputs = Inputs(resolved, unresolved),
                    outputs = outputs,
                    inputParties = resolved.getParties(),
                    outputParties = outputs.getParties(),
                    commandTypes = it.transaction.tx.commands.map { it.value.javaClass },
                    totalValueEquiv = ::calculateTotalEquiv.lift(myIdentity,
                            reportingExchange,
                            resolved.map { it.state.data }.lift(),
                            it.transaction.tx.outputs.map { it.data }.lift())
            )
        }

        val searchField = SearchField(transactions,
                "Transaction ID" to { tx, s -> "${tx.id}".contains(s, true) },
                "Input" to { tx, s -> tx.inputs.resolved.any { it.state.data.contract.javaClass.simpleName.contains(s, true) } },
                "Output" to { tx, s -> tx.outputs.any { it.state.data.contract.javaClass.simpleName.contains(s, true) } },
                "Input Party" to { tx, s -> tx.inputParties.any { it.any { it.value?.legalIdentity?.name?.commonName?.contains(s, true) ?: false } } },
                "Output Party" to { tx, s -> tx.outputParties.any { it.any { it.value?.legalIdentity?.name?.commonName?.contains(s, true) ?: false } } },
                "Command Type" to { tx, s -> tx.commandTypes.any { it.simpleName.contains(s, true) } }
        )
        root.top = searchField.root
        // Transaction table
        transactionViewTable.apply {
            items = searchField.filteredData
            column("Transaction ID", Transaction::id) { maxWidth = 200.0 }.setCustomCellFactory {
                label("$it") {
                    graphic = identicon(it, 15.0)
                    tooltip = identiconToolTip(it)
                }
            }
            column("Input", Transaction::inputs).cellFormat {
                text = it.resolved.toText()
                if (!it.unresolved.isEmpty()) {
                    if (!text.isBlank()) {
                        text += ", "
                    }
                    text += "Unresolved(${it.unresolved.size})"
                }
            }
            column("Output", Transaction::outputs).cellFormat { text = it.toText() }
            column("Input Party", Transaction::inputParties).cellFormat { text = it.flatten().map { it.value?.legalIdentity?.name }.filterNotNull().toSet().joinToString() }
            column("Output Party", Transaction::outputParties).cellFormat { text = it.flatten().map { it.value?.legalIdentity?.name }.filterNotNull().toSet().joinToString() }
            column("Command type", Transaction::commandTypes).cellFormat { text = it.map { it.simpleName }.joinToString() }
            column("Total value", Transaction::totalValueEquiv).cellFormat {
                text = "${it.positivity.sign}${AmountFormatter.boring.format(it.amount)}"
                titleProperty.bind(reportingCurrency.map { "Total value ($it equiv)" })
            }

            rowExpander {
                add(ContractStatesView(it).root)
                prefHeight = 400.0
            }.apply {
                prefWidth = 26.0
                isResizable = false
            }
            setColumnResizePolicy { true }
        }
        matchingTransactionsLabel.textProperty().bind(Bindings.size(transactionViewTable.items).map {
            "$it matching transaction${if (it == 1) "" else "s"}"
        })
    }

    private fun ObservableList<StateAndRef<ContractState>>.getParties() = map { it.state.data.participants.map { getModel<NetworkIdentityModel>().lookup(it) } }
    private fun ObservableList<StateAndRef<ContractState>>.toText() = map { it.contract().javaClass.simpleName }.groupBy { it }.map { "${it.key} (${it.value.size})" }.joinToString()

    private class TransactionWidget : BorderPane() {
        private val partiallyResolvedTransactions by observableListReadOnly(TransactionDataModel::partiallyResolvedTransactions)

        // TODO : Add a scrolling table to show latest transaction.
        // TODO : Add a chart to show types of transactions.
        init {
            right {
                label {
                    textProperty().bind(Bindings.size(partiallyResolvedTransactions).map(Number::toString))
                    BorderPane.setAlignment(this, Pos.BOTTOM_RIGHT)
                }
            }
        }
    }

    private inner class ContractStatesView(transaction: Transaction) : Fragment() {
        override val root by fxml<Parent>()
        private val inputs by fxid<ListView<StateAndRef<ContractState>>>()
        private val outputs by fxid<ListView<StateAndRef<ContractState>>>()
        private val signatures by fxid<VBox>()
        private val inputPane by fxid<TitledPane>()
        private val outputPane by fxid<TitledPane>()
        private val signaturesPane by fxid<TitledPane>()

        init {
            val signatureData = transaction.tx.transaction.sigs.map { it.by }
            // Bind count to TitlePane
            inputPane.text = "Input (${transaction.inputs.resolved.count()})"
            outputPane.text = "Output (${transaction.outputs.count()})"
            signaturesPane.text = "Signatures (${signatureData.count()})"

            inputs.cellCache { getCell(it) }
            outputs.cellCache { getCell(it) }

            inputs.items = transaction.inputs.resolved
            outputs.items = transaction.outputs.observable()

            signatures.children.addAll(signatureData.map { signature ->
                val nodeInfo = getModel<NetworkIdentityModel>().lookup(signature)
                copyableLabel(nodeInfo.map { "${signature.toStringShort()} (${it?.legalIdentity?.name ?: "???"})" })
            })
        }

        private fun getCell(contractState: StateAndRef<ContractState>): Node {
            return {
                gridpane {
                    padding = Insets(0.0, 5.0, 10.0, 10.0)
                    vgap = 10.0
                    hgap = 10.0
                    row {
                        label("${contractState.contract().javaClass.simpleName} (${contractState.ref.toString().substring(0, 16)}...)[${contractState.ref.index}]") {
                            graphic = identicon(contractState.ref.txhash, 30.0)
                            tooltip = identiconToolTip(contractState.ref.txhash)
                            gridpaneConstraints { columnSpan = 2 }
                        }
                    }
                    val data = contractState.state.data
                    when (data) {
                        is Cash.State -> {
                            row {
                                label("Amount :") { gridpaneConstraints { hAlignment = HPos.RIGHT } }
                                label(AmountFormatter.boring.format(data.amount.withoutIssuer()))
                            }
                            row {
                                label("Issuer :") { gridpaneConstraints { hAlignment = HPos.RIGHT } }
                                val anonymousIssuer: AnonymousParty = data.amount.token.issuer.party
                                val issuer: AbstractParty = anonymousIssuer.resolveIssuer().value ?: anonymousIssuer
                                // TODO: Anonymous should probably be italicised or similar
                                label(issuer.nameOrNull()?.commonName ?: "Anonymous") {
                                    tooltip(anonymousIssuer.owningKey.toBase58String())
                                }
                            }
                            row {
                                label("Owner :") { gridpaneConstraints { hAlignment = HPos.RIGHT } }
                                val owner = data.owner
                                val nodeInfo = getModel<NetworkIdentityModel>().lookup(owner)
                                label(nodeInfo.map { it?.legalIdentity?.name ?: "???" }) {
                                    tooltip(data.owner.toBase58String())
                                }
                            }
                        }
                    // TODO : Generic view using reflection?
                        else -> label {}
                    }
                }
            }()
        }
    }

    private fun StateAndRef<ContractState>.contract() = this.state.data.contract
}

/**
 * We calculate the total value by subtracting relevant input states and adding relevant output states, as long as they're cash
 */
private fun calculateTotalEquiv(identity: NodeInfo?,
                                reportingCurrencyExchange: Pair<Currency, (Amount<Currency>) -> Amount<Currency>>,
                                inputs: List<ContractState>,
                                outputs: List<ContractState>): AmountDiff<Currency> {
    val (reportingCurrency, exchange) = reportingCurrencyExchange
    val publicKey = identity?.legalIdentity?.owningKey
    fun List<ContractState>.sum() = this.map { it as? Cash.State }
            .filterNotNull()
            .filter { publicKey == it.owner }
            .map { exchange(it.amount.withoutIssuer()).quantity }
            .sum()

    // For issuing cash, if I am the issuer and not the owner (e.g. issuing cash to other party), count it as negative.
    val issuedAmount = if (inputs.isEmpty()) outputs.map { it as? Cash.State }
            .filterNotNull()
            .filter { publicKey == it.amount.token.issuer.party.owningKey && publicKey != it.owner }
            .map { exchange(it.amount.withoutIssuer()).quantity }
            .sum() else 0

    return AmountDiff.fromLong(outputs.sum() - inputs.sum() - issuedAmount, reportingCurrency)
}
