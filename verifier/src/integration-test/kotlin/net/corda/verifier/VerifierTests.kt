package net.corda.verifier

import com.google.common.util.concurrent.Futures
import net.corda.client.mock.generateOrFail
import net.corda.core.contracts.DOLLARS
import net.corda.core.crypto.X509Utilities
import net.corda.core.map
import net.corda.core.messaging.startFlow
import net.corda.core.node.services.ServiceInfo
import net.corda.core.serialization.OpaqueBytes
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.transactions.WireTransaction
import net.corda.core.utilities.ALICE
import net.corda.core.utilities.DUMMY_NOTARY
import net.corda.flows.CashIssueFlow
import net.corda.flows.CashPaymentFlow
import net.corda.node.services.config.VerifierType
import net.corda.node.services.transactions.ValidatingNotaryService
import org.junit.Test
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

class VerifierTests {
    private fun generateTransactions(number: Int): List<LedgerTransaction> {
        var currentLedger = GeneratedLedger.empty
        val transactions = ArrayList<WireTransaction>()
        val random = SplittableRandom()
        for (i in 0..number - 1) {
            val (tx, ledger) = currentLedger.transactionGenerator.generateOrFail(random)
            transactions.add(tx)
            currentLedger = ledger
        }
        return transactions.map { currentLedger.resolveWireTransaction(it) }
    }

    @Test
    fun `single verifier works with requestor`() {
        verifierDriver(automaticallyStartNetworkMap = false) {
            val aliceFuture = startVerificationRequestor(ALICE.name)
            val transactions = generateTransactions(100)
            val alice = aliceFuture.get()
            startVerifier(alice)
            alice.waitUntilNumberOfVerifiers(1)
            val results = Futures.allAsList(transactions.map { alice.verifyTransaction(it) }).get()
            results.forEach {
                if (it != null) {
                    throw it
                }
            }
        }
    }

    @Test
    fun `multiple verifiers work with requestor`() {
        verifierDriver(automaticallyStartNetworkMap = false) {
            val aliceFuture = startVerificationRequestor(ALICE.name)
            val transactions = generateTransactions(100)
            val alice = aliceFuture.get()
            val numberOfVerifiers = 4
            for (i in 1..numberOfVerifiers) {
                startVerifier(alice)
            }
            alice.waitUntilNumberOfVerifiers(numberOfVerifiers)
            val results = Futures.allAsList(transactions.map { alice.verifyTransaction(it) }).get()
            results.forEach {
                if (it != null) {
                    throw it
                }
            }
        }
    }

    @Test
    fun `verification redistributes on verifier death`() {
        verifierDriver(automaticallyStartNetworkMap = false) {
            val aliceFuture = startVerificationRequestor(ALICE.name)
            val numberOfTransactions = 100
            val transactions = generateTransactions(numberOfTransactions)
            val alice = aliceFuture.get()
            val verifier1 = startVerifier(alice)
            val verifier2 = startVerifier(alice)
            startVerifier(alice)
            alice.waitUntilNumberOfVerifiers(3)
            val remainingTransactionsCount = AtomicInteger(numberOfTransactions)
            val futures = transactions.map { transaction ->
                val future = alice.verifyTransaction(transaction)
                // Kill verifiers as results are coming in, forcing artemis to redistribute.
                future.map {
                    val remaining = remainingTransactionsCount.decrementAndGet()
                    when (remaining) {
                        33 -> verifier1.get().process.destroy()
                        66 -> verifier2.get().process.destroy()
                    }
                    it
                }
            }
            Futures.allAsList(futures).get()
        }
    }

    @Test
    fun `verification request waits until verifier comes online`() {
        verifierDriver(automaticallyStartNetworkMap = false) {
            val aliceFuture = startVerificationRequestor(ALICE.name)
            val transactions = generateTransactions(100)
            val alice = aliceFuture.get()
            val futures = transactions.map { alice.verifyTransaction(it) }
            startVerifier(alice)
            Futures.allAsList(futures).get()
        }
    }

    @Test
    fun `single verifier works with a node`() {
        verifierDriver {
            val aliceFuture = startNode(ALICE.name)
            val notaryFuture = startNode(DUMMY_NOTARY.name, advertisedServices = setOf(ServiceInfo(ValidatingNotaryService.type)), verifierType = VerifierType.OutOfProcess)
            val alice = aliceFuture.get()
            val notary = notaryFuture.get()
            startVerifier(notary)
            alice.rpc.startFlow(::CashIssueFlow, 10.DOLLARS, OpaqueBytes.of(0), alice.nodeInfo.legalIdentity, notaryFuture.get().nodeInfo.notaryIdentity).returnValue.get()
            notary.waitUntilNumberOfVerifiers(1)
            for (i in 1..10) {
                alice.rpc.startFlow(::CashPaymentFlow, 10.DOLLARS, alice.nodeInfo.legalIdentity).returnValue.get()
            }
        }
    }
}