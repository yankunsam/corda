package net.corda.irs.plugin

import net.corda.core.contracts.StateRef
import net.corda.core.crypto.Party
import net.corda.core.node.CordaPluginRegistry
import net.corda.core.transactions.SignedTransaction
import net.corda.flows.CollectSignaturesFlow
import net.corda.irs.api.InterestRateSwapAPI
import net.corda.irs.contract.InterestRateSwap
import net.corda.irs.flows.*
import java.time.Duration
import java.time.LocalDate
import java.util.function.Function

class IRSPlugin : CordaPluginRegistry() {
    override val webApis = listOf(Function(::InterestRateSwapAPI))
    override val staticServeDirs: Map<String, String> = mapOf(
            "irsdemo" to javaClass.classLoader.getResource("irsweb").toExternalForm()
    )
    override val servicePlugins = listOf(Function(FixingFlow::Service), Function(CollectSignatureFlowImpl::Service))
    override val requiredFlows: Map<String, Set<String>> = mapOf(
            AutoOfferFlow.Requester::class.java.name to setOf(InterestRateSwap.State::class.java.name),
            UpdateBusinessDayFlow.Broadcast::class.java.name to setOf(LocalDate::class.java.name),
            ExitServerFlow.Broadcast::class.java.name to setOf(kotlin.Int::class.java.name),
            FixingFlow.FixingRoleDecider::class.java.name to setOf(StateRef::class.java.name, Duration::class.java.name),
            FixingFlow.Floater::class.java.name to setOf(Party::class.java.name, FixingFlow.FixingSession::class.java.name),
            CollectSignaturesFlow::class.java.name to setOf(SignedTransaction::class.java.name))
}
