package net.corda.node.services.persistence

import net.corda.contracts.CommercialPaper
import net.corda.contracts.asset.Cash
import net.corda.core.contracts.*
import net.corda.core.crypto.CompositeKey
import net.corda.core.crypto.Party
import net.corda.core.crypto.SecureHash
import net.corda.core.days
import net.corda.core.node.services.NonQueryableStateException
import net.corda.core.schemas.MappedSchema
import net.corda.core.schemas.PersistentState
import net.corda.core.schemas.QueryableState
import net.corda.core.utilities.DUMMY_NOTARY
import net.corda.core.utilities.TEST_TX_TIME
import net.corda.node.services.query.RequeryQueryServiceImpl
import net.corda.node.services.schema.NodeSchemaService
import net.corda.node.utilities.configureDatabase
import net.corda.node.utilities.databaseTransaction
import net.corda.schemas.CashSchemaV1
import net.corda.schemas.CommercialPaperSchemaV1
import net.corda.schemas.TradeSchemaV1
import net.corda.testing.*
import net.corda.testing.node.MockServices
import net.corda.testing.node.makeTestDataSourceProperties
import org.jetbrains.exposed.sql.Database
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.io.Closeable
import java.security.PublicKey
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.fail
import net.corda.schemas.Models

class RequeryPersistenceServiceTest {

    lateinit var dataSource: Closeable
    lateinit var database: Database

    lateinit var services: MockServices
    val persistenceService = RequeryPersistenceService(NodeSchemaService(), Models)
    val queryService = RequeryQueryServiceImpl(NodeSchemaService(), Models)

    @Before
    fun setUp() {
        val dataSourceAndDatabase = configureDatabase(makeTestDataSourceProperties())
        dataSource = dataSourceAndDatabase.first
        database = dataSourceAndDatabase.second
    }

    @After
    fun tearDown() {
        dataSource.close()
    }

    /**
     *  Cash Schema Tests
     */
    val TEST_SCHEMA_CASH = CashSchemaV1
    val TEST_ENTITY_CASH = CashSchemaV1.PersistentCashState::class

    private fun makeCash(amount: Amount<Currency>, owner: CompositeKey, issuerParty: Party = BOC, issuerRef: Byte = 1) =
        StateAndRef(
                Cash.State(amount `issued by` issuerParty.ref(issuerRef), owner) `with notary` DUMMY_NOTARY,
                StateRef(SecureHash.randomSHA256(), Random().nextInt(32))
        )

    @Test
    fun persistCashState() {
        val state = makeCash(100.DOLLARS, MEGA_CORP_PUBKEY)

        databaseTransaction(database) {
            persistenceService.persistState(state)
            val results = queryService.criteriaQueryForSchema(TEST_SCHEMA_CASH, TEST_ENTITY_CASH)
            assertEquals(1, results.count())
        }
    }

    @Test
    fun persistCash() {
        val states = setOf(
                makeCash(100.DOLLARS, MEGA_CORP_PUBKEY),
                makeCash(400.DOLLARS, MEGA_CORP_PUBKEY),
                makeCash(80.DOLLARS, MINI_CORP_PUBKEY),
                makeCash(80.SWISS_FRANCS, MINI_CORP_PUBKEY, BOC, 2)
        )

        databaseTransaction(database) {
            persistenceService.persist(states)

            val results = queryService.criteriaQueryForSchema(TEST_SCHEMA_CASH, TEST_ENTITY_CASH)
            assertEquals(4, results.count())
        }
    }

    /**
     *  Commercial Paper Schema Tests
     */
    val TEST_SCHEMA_CP  = CommercialPaperSchemaV1
    val TEST_ENTITY_CP  = CommercialPaperSchemaV1.PersistentCommericalPaperState::class

    private fun makeCommercialPaper() =
        StateAndRef(
            CommercialPaper.State(
                    issuance = MEGA_CORP.ref(123),
                    owner = MEGA_CORP_PUBKEY,
                    faceValue = 1000.DOLLARS `issued by` MEGA_CORP.ref(123),
                    maturityDate = TEST_TX_TIME + 7.days
            ) `with notary` DUMMY_NOTARY,
            StateRef(SecureHash.randomSHA256(), Random().nextInt(32))
        )

    @Test
    fun persistCommercialPaperState() {
        val state = makeCommercialPaper()

        databaseTransaction(database) {
            persistenceService.persistState(state)
            val results = queryService.criteriaQueryForSchema(TEST_SCHEMA_CP, TEST_ENTITY_CP)
            assertEquals(1, results.count())
        }
    }

    /**
     *  Generic Trade Schema Tests
     */
    val TEST_SCHEMA_TRADE  = TradeSchemaV1
    val TEST_ENTITY_TRADE  = TradeSchemaV1.PersistentTradeState::class

    class TradeContract : DummyContract() {
        data class State(
            val party1: String,
            var party2: String,
            var tradeIdParty1: String,
            var tradeIdParty2: String,
            var tradeDate: Instant
        ) : LinearState, QueryableState {

            override val contract: Contract = TradeContract()
            override val participants: List<CompositeKey>
                get() = throw UnsupportedOperationException()

            override val linearId: UniqueIdentifier
                get() = throw UnsupportedOperationException()

            override fun isRelevant(ourKeys: Set<PublicKey>): Boolean {
                throw UnsupportedOperationException("not implemented") //To change body of created functions use File | Settings | File Templates.
            }
            /** Object Relational Mapping support. */
            override fun supportedSchemas(): Iterable<MappedSchema> = listOf(TradeSchemaV1)

            /** Object Relational Mapping support. */
            override fun generateMappedObject(schema: MappedSchema): PersistentState {
                return when (schema) {
                    is TradeSchemaV1 -> TradeSchemaV1.PersistentTradeState(
                            party1 = this.party1,
                            party2 = this.party2,
                            tradeIdParty1 = this.tradeIdParty1,
                            tradeIdParty2 = this.tradeIdParty2,
                            tradeDate = this.tradeDate
                    )
                    else -> throw IllegalArgumentException("Unrecognised schema $schema")
                }
            }
        }
    }

    /**
     * http://www.fpml.org/spec/fpml-5-9-2-wd-2/html/confirmation/xml/products/interest-rate-derivatives/ird-ex01-vanilla-swap.xml
     **/
    private fun makeTrade() =
        StateAndRef(
                TradeContract.State(
                        party1 = "PARTYAUS33",
                        party2 = "BARCGB2L",
                        tradeIdParty1 = "TW9235",
                        tradeIdParty2 = "SW2000",
                        tradeDate = LocalDate.parse("1994-12-12").atStartOfDay().toInstant(ZoneOffset.UTC)
                ) `with notary` DUMMY_NOTARY,
                StateRef(SecureHash.randomSHA256(), Random().nextInt(32))
        )

    @Test
    fun persistTradeState() {
        val state = makeTrade()

        databaseTransaction(database) {
            persistenceService.persistState(state)
            val results = queryService.criteriaQueryForSchema(TEST_SCHEMA_TRADE, TEST_ENTITY_TRADE)
            assertEquals(1, results.count())
        }
    }

    /**
     * Failure test scenarios
     */
    @Test(expected = NonQueryableStateException::class)
    fun attemptPersistNonQueryableState() {

        val inState = TransactionState(DummyContract.SingleOwnerState(0, ALICE_PUBKEY), DUMMY_NOTARY)
        val stateAndRef = StateAndRef(inState, StateRef(SecureHash.randomSHA256(), 0))

        databaseTransaction(database) {
            persistenceService.persistState(stateAndRef)
            fail()
        }
    }

}
