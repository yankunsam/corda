package net.corda.node.services.vault

import net.corda.core.contracts.StateRef
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.StateMachineRunId
import net.corda.core.node.services.VaultService
import net.corda.core.utilities.loggerFor
import net.corda.core.utilities.trace
import net.corda.node.services.statemachine.FlowStateMachineImpl
import net.corda.node.services.statemachine.StateMachineManager
import net.corda.node.utilities.AddOrRemoveError
import java.util.*

class VaultSoftLockManager(val vault: VaultService, smm: StateMachineManager) {

    private companion object {
        val log = loggerFor<VaultSoftLockManager>()
    }

    init {
        smm.changes.subscribe { (logic, addOrRemove, id) ->
            if (addOrRemove is AddOrRemoveError.Remove && (FlowStateMachineImpl.currentStateMachine())?.hasSoftLockedStates == true) {
                log.trace { "$addOrRemove Flow name ${logic.javaClass} with id $id" }
                unregisterSoftLocks(id, logic)
            }
        }

        // Discussion
        //
        // The intent of the following approach is to support what might be a common pattern in a flow:
        //      1. Create state
        //      2. Do something with state
        //  without possibility of another flow intercepting the state between 1 and 2,
        //  since we cannot lock the state before it exists. e.g. Issue and then Move some Cash.
        //
        //  The downside is we could have a long running flow that holds a lock for a long period of time.
        //  However, the lock can be programmatically released, like any other soft lock,
        //  should we want a long running flow that creates a visible state mid way through.

        vault.rawUpdates.subscribe { update ->
            update.flowId?.let {
                if (update.produced.isNotEmpty()) {
                    registerSoftLocks(update.flowId as UUID, update.produced.map { it.ref })
                }
            }
        }
    }

    private fun registerSoftLocks(flowId: UUID, stateRefs: List<StateRef>) {
        log.trace("Reserving soft locks for flow id $flowId and states $stateRefs")
        vault.softLockReserve(flowId, stateRefs.toSet())
    }

    private fun unregisterSoftLocks(id: StateMachineRunId, logic: FlowLogic<*>) {
        val flowClassName = logic.javaClass.simpleName
        log.trace("Releasing soft locks for flow $flowClassName with flow id ${id.uuid}")
        vault.softLockRelease(id.uuid)

    }
}