package net.corda.node.services.persistence

import com.r3corda.node.services.query.HibernateQueryServiceImpl
import com.r3corda.node.services.schema.HibernatePersistenceService
import net.corda.contracts.asset.Cash
import net.corda.core.contracts.*
import net.corda.core.crypto.CompositeKey
import net.corda.core.crypto.Party
import net.corda.core.crypto.SecureHash
import net.corda.core.node.services.NonQueryableStateException
import net.corda.core.utilities.DUMMY_NOTARY
import net.corda.node.services.schema.NodeSchemaService
import net.corda.node.utilities.configureDatabase
import net.corda.node.utilities.databaseTransaction
import net.corda.schemas.CashSchemaV1
import net.corda.testing.*
import net.corda.testing.node.MockServices
import net.corda.testing.node.makeTestDataSourceProperties
import org.jetbrains.exposed.sql.Database
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.io.Closeable
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.fail

class HibernatePersistenceServiceTest {

    lateinit var dataSource: Closeable
    lateinit var database: Database

    lateinit var services: MockServices
    val persistenceService = HibernatePersistenceService(NodeSchemaService())
    val queryService = HibernateQueryServiceImpl(NodeSchemaService())

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

    @Test
    fun persistState() {

        val CASH_STATE = makeCash(100.DOLLARS, MEGA_CORP_PUBKEY)

        databaseTransaction(database) {
            persistenceService.persistState(CASH_STATE)

            val results = queryService.nativeQueryForSchema("SELECT * FROM cash_states", CashSchemaV1)
            assertEquals(1, results.count())
        }
    }

    @Test
    fun persist() {

        val CASH_STATES = setOf(
                makeCash(100.DOLLARS, MEGA_CORP_PUBKEY),
                makeCash(400.DOLLARS, MEGA_CORP_PUBKEY),
                makeCash(80.DOLLARS, MINI_CORP_PUBKEY),
                makeCash(80.SWISS_FRANCS, MINI_CORP_PUBKEY, BOC, 2)
        )

        databaseTransaction(database) {
            persistenceService.persist(CASH_STATES)

            val results = queryService.nativeQueryForSchema("SELECT * FROM cash_states", CashSchemaV1)
            assertEquals(4, results.count())
        }
    }

    @Test(expected = NonQueryableStateException::class)
    fun attemptPersistNonQueryableState() {

        val inState = TransactionState(DummyContract.SingleOwnerState(0, ALICE_PUBKEY), DUMMY_NOTARY)
        val stateAndRef = StateAndRef(inState, StateRef(SecureHash.randomSHA256(), 0))

        databaseTransaction(database) {
            persistenceService.persistState(stateAndRef)
            fail()
        }
    }

    private fun makeCash(amount: Amount<Currency>, owner: CompositeKey, issuerParty: Party = BOC, issuerRef: Byte = 1) =
            StateAndRef(
                    Cash.State(amount `issued by` issuerParty.ref(issuerRef), owner) `with notary` DUMMY_NOTARY,
                    StateRef(SecureHash.randomSHA256(), Random().nextInt(32))
            )
}