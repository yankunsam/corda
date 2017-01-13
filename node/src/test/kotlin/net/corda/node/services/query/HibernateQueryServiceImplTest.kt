package com.r3corda.node.services.query

import com.r3corda.testing.vault.AbstractVaultTest
import net.corda.node.utilities.databaseTransaction
import net.corda.schemas.CashSchemaV1
import org.junit.Test
import kotlin.test.assertEquals

class HibernateQueryServiceImplTest : AbstractVaultTest() {

    val TEST_SCHEMA  = CashSchemaV1

    val SIMPLE_QUERY = "SELECT c FROM CashSchemaV1\$PersistentCashState c WHERE c.currency = ?1"
    val SIMPLE_QUERY_NAMED = "SELECT c FROM CashSchemaV1\$PersistentCashState c WHERE c.currency = :currency"

    val NAMED_QUERY  = "cash.findByCurrency"

    // note: Named parameters are not supported by JPA in native queries, only for JPQL. You must use positional parameters.
    val NATIVE_QUERY = "SELECT * FROM cash_states WHERE ccy_code = ?1"

    val TEST_ENTITY  = CashSchemaV1.PersistentCashState::class

    /**
     *  AbstractVaultTest setUp inserts 3 transactions in the Vault
     *  (one for each currency: GBP, USD, CHF)
     */

    @Test
    fun simpleQueryForSchema() {

        databaseTransaction(database) {
            // JPA Query returns List<Entity>
            // using Positional args
            val results = queryService.simpleQueryForSchema(SIMPLE_QUERY, TEST_SCHEMA, "GBP")
            assertEquals(1, results.count())

            // JPA Query returns List<Entity>
            // using Named args
            val results2 = queryService.simpleQueryForSchemaUsingNamedArgs(SIMPLE_QUERY_NAMED, TEST_SCHEMA, "currency", "GBP")
            assertEquals(1, results2.count())
        }
    }

    @Test
    fun namedQueryForSchema() {
        databaseTransaction(database) {
            // JPA Query returns List<Entity>
            val results = queryService.namedQueryForSchema(NAMED_QUERY, TEST_SCHEMA, "GBP") as List<CashSchemaV1.PersistentCashState>
            assertEquals(1, results.count())
        }
    }

    @Test
    fun nativeQueryForSchema() {
        databaseTransaction(database) {
            // Native Query returns List<Object>
            val results = queryService.nativeQueryForSchema(NATIVE_QUERY, TEST_SCHEMA, "GBP")
            assertEquals(1, results.count())
        }
    }

    @Test
    fun criteriaQueryForSchema() {
        val results = queryService.criteriaQueryForSchema(TEST_SCHEMA, TEST_ENTITY, "currency", "GBP")
        assertEquals(1, results.count())
    }

}