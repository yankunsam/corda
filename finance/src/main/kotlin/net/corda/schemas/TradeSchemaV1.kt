package net.corda.schemas

import io.requery.Convert
import net.corda.core.schemas.MappedSchema
import net.corda.core.schemas.PersistentState
import net.corda.core.contracts.LinearState
import net.corda.core.schemas.requery.converters.InstantConverter
import java.time.Instant
import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.Table

object TradeSchema

/**
 *  First version of a generic trade ORM schema for usage by Contract's that extend the [LinearState] type
 *  Attributes defined below represent a subset of mandatory FpML Trader Header schema attributes.
 *  See http://www.fpml.org/spec/fpml-5-9-2-wd-2/
 */
object TradeSchemaV1 : MappedSchema(schemaFamily = TradeSchema.javaClass, version = 1, mappedTypes = listOf(PersistentTradeState::class.java)) {
    @Entity
    @Table(name = "trade_states")
    class PersistentTradeState(

        @Column(name = "party1")
        var party1: String,

        @Column(name = "party2")
        var party2: String,

        @Column(name = "trade_id_party1")
        var tradeIdParty1: String,

        @Column(name = "trade_id_party2")
        var tradeIdParty2: String,

        @Convert(InstantConverter::class)
        @Column(name = "tradeDate")
        var tradeDate: Instant
    ) : PersistentState()
}