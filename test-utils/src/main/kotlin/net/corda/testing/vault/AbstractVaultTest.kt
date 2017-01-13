package com.r3corda.testing.vault

import com.r3corda.node.services.query.HibernateQueryServiceImpl
import com.r3corda.node.services.schema.HibernatePersistenceService
import net.corda.contracts.testing.fillWithSomeTestCash
import net.corda.core.contracts.DOLLARS
import net.corda.core.contracts.POUNDS
import net.corda.core.contracts.SWISS_FRANCS
import net.corda.core.node.services.QueryService
import net.corda.core.node.services.VaultService
import net.corda.core.transactions.SignedTransaction
import net.corda.node.services.schema.NodeSchemaService
import net.corda.node.services.vault.NodeVaultService
import net.corda.node.utilities.configureDatabase
import net.corda.node.utilities.databaseTransaction
import net.corda.testing.node.MockServices
import net.corda.testing.node.makeTestDataSourceProperties
import org.jetbrains.exposed.sql.Database
import org.junit.After
import org.junit.Before
import java.io.Closeable

open class AbstractVaultTest {
    lateinit var dataSource: Closeable
    lateinit var database: Database

    lateinit var services: MockServices
    open val vaultService: VaultService get() = services.vaultService
    open val queryService: QueryService get() = services.queryService

    @Before
    open fun setUp() {
        val dataSourceAndDatabase = configureDatabase(makeTestDataSourceProperties())
        dataSource = dataSourceAndDatabase.first
        database = dataSourceAndDatabase.second

        databaseTransaction(database) {
            services = object : MockServices() {
                override val vaultService = NodeVaultService(this)
                override val persistenceService = HibernatePersistenceService(NodeSchemaService())
                override val queryService = HibernateQueryServiceImpl(NodeSchemaService())

                init {
                    vaultService.updates.subscribe { persistenceService.persist(it.produced) }
                }

                override fun recordTransactions(txs: Iterable<SignedTransaction>) {
                    for (stx in txs) {
                        storageService.validatedTransactions.addTransaction(stx)
                        vaultService.notify(stx.tx)
                    }
                }
            }
            services.fillWithSomeTestCash(100.POUNDS, atMostThisManyStates = 1, atLeastThisManyStates = 1)
            services.fillWithSomeTestCash(200.DOLLARS, atMostThisManyStates = 1, atLeastThisManyStates = 1)
            services.fillWithSomeTestCash(300.SWISS_FRANCS , atMostThisManyStates = 1, atLeastThisManyStates = 1)
        }
    }

    @After
    fun tearDown() {
        dataSource.close()
    }
}