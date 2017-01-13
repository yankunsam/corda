package net.corda.node.services.vault.schemas

import io.requery.Key
import io.requery.Persistable
import io.requery.Superclass

import javax.persistence.Column

object Requery {
    /**
     * A super class for all mapped states exported to a schema that ensures the [StateRef] appears on the database row.  The
     * [StateRef] will be set to the correct value by the framework (there's no need to set during mapping generation by the state itself).
     */
    @Superclass interface PersistentState : Persistable {
        @get:Key
        @get:Column(name = "transaction_id", length = 64)
        var txId: String

        @get:Key
        @get:Column(name = "output_index")
        var index: Int
    }
}
