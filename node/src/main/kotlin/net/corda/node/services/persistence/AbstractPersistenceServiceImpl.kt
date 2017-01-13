package net.corda.node.services.persistence

import net.corda.core.contracts.ContractState
import net.corda.core.contracts.StateAndRef
import net.corda.core.node.services.NonQueryableStateException
import net.corda.core.node.services.PersistenceService
import net.corda.core.schemas.QueryableState
import net.corda.core.utilities.debug
import net.corda.core.utilities.loggerFor
import net.corda.node.services.api.SchemaService

abstract class AbstractPersistenceServiceImpl(open val schemaService: SchemaService) : PersistenceService {

    companion object {
        val logger = loggerFor<AbstractPersistenceServiceImpl>()
    }

    override fun persist(stateAndRefs: Set<StateAndRef<ContractState>>) {
        stateAndRefs.forEach { persistState(it) }
    }

    override fun persistState(stateAndRef: StateAndRef<ContractState>) {
        val state = stateAndRef.state.data
        if (state is QueryableState) {
            logger.debug { "Asked to persist state ${stateAndRef.ref}" }
            schemaService.selectSchemas(state).forEach { persistStateWithSchema(state, stateAndRef.ref, it) }
        }
        else
            throw NonQueryableStateException("${state.javaClass} does not implement the QueryableState interface")
    }
}