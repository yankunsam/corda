package net.corda.node.services.vault.schemas

import io.requery.*
import net.corda.core.node.services.Vault
import net.corda.core.schemas.requery.Requery
import java.time.Instant
import java.util.*

object VaultSchema {

    @Table(name = "vault_transaction_notes")
    @Entity(model = "vault")
    interface VaultTxnNote : Persistable {
        @get:Key
        @get:Generated
        @get:Column(name = "seq_no", index = true)
        var seqNo: Int

        @get:Column(name = "transaction_id", length = 64, index = true)
        var txId: String

        @get:Column(name = "note")
        var note: String
    }

    @Table(name = "vault_cash_balances")
    @Entity(model = "vault")
    interface VaultCashBalances : Persistable {
        @get:Key
        @get:Column(name = "currency_code", length = 3)
        var currency: String

        @get:Column(name = "amount", value = "0")
        var amount: Long
    }

    @Table(name = "vault_states")
    @Entity(model = "vault")
    interface VaultStates : Requery.PersistentState {
        /** refers to the notary a state is attached to */
        @get:Column(name = "notary_name")
        var notaryName: String

        @get:Column(name = "notary_key", length = 65535) // TODO What is the upper limit on size of CompositeKey?
        var notaryKey: String

        /** references a concrete ContractState that is [QueryableState] and has a [MappedSchema] */
        @get:Column(name = "contract_state_class_name")
        var contractStateClassName: String

        /** refers to serialized transaction Contract State */
        // TODO: define contract state size maximum size and adjust length accordingly
        @get:Column(name = "contract_state", length = 100000)
        var contractState: ByteArray

        /** state lifecycle: unconsumed, consumed */
        @get:Column(name = "state_status")
        var stateStatus: Vault.StateStatus

        /** refers to timestamp recorded upon entering UNCONSUMED state */
        @get:Column(name = "recorded_timestamp")
        var recordedTime: Instant

        /** refers to timestamp recorded upon entering CONSUMED state */
        @get:Column(name = "consumed_timestamp", nullable = true)
        var consumedTime: Instant?

        /** used to denote a state has been soft locked (to prevent double spend)
         *  will contain a temporary unique [UUID] obtained from a flow session */
        @get:Column(name = "lock_id", nullable = true)
        var lockId: String?

        /** refers to the last time a lock was taken (reserved) or updated (released, re-reserved) */
        @get:Column(name = "lock_timestamp", nullable = true)
        var lockUpdateTime: Instant?
    }

    @Table(name = "vault_linear_states")
    @Entity(model = "vault")
//    interface VaultLinearState : Requery.PersistentState {
      interface VaultLinearState : Persistable {

        @get:Key
        @get:Generated
        @get:Column(name = "id")
        var id: Int

        /** refers to one or many Anonymous parties who may be able to consume a state in a transaction */
//        @get:OneToMany(mappedBy = "key")
//        var participants: Set<VaultParty>

        /**
         * [LinearState] attributes
         */

        @get:Index("external_id_index")
        @get:Column(name = "external_id")
        var externalId: String

        @get:Column(name = "uuid", unique = true, nullable = false)
        var uuid: UUID

        /**
         * [DealState] attributes
         */

        @get:Index("deal_reference_index")
        @get:Column(name = "deal_reference")
        var dealRef: String

        @get:OneToMany(mappedBy = "linear_state_parties")
        var dealParties: Set<VaultParty>

//        @get:ForeignKey
//        @get:OneToOne
//        var owner: VaultParty

    }

    @Table(name = "vault_party")
    @Entity(model = "vault")
    interface VaultParty : Persistable  {

        @get:Key
        @get:Generated
        @get:Column(name = "id")
        var id: Int

        // NOTE: remove this back reference when Requery supports Unidirectional relationships
        @get:ManyToOne
        @get:Column(name = "linear_state_parties")
        val linearStateParties: VaultLinearState

        /**
         * [Party] attributes
         */
        @get:Column(name = "party_name")
        var name: String

        @get:Column(name = "party_key")
        var key: String

        // NOTE: remove this back reference when Requery supports Unidirectional relationships
//        @get:OneToOne
//        val owner: VaultLinearState
    }
}