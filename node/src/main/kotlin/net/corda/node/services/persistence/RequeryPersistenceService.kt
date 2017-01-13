package net.corda.node.services.persistence

import com.r3corda.node.services.database.RequeryConfiguration
import io.requery.meta.EntityModel
import net.corda.core.contracts.StateRef
import net.corda.core.schemas.MappedSchema
import net.corda.core.schemas.PersistentStateRef
import net.corda.core.schemas.QueryableState
import net.corda.core.utilities.loggerFor
import net.corda.node.services.api.SchemaService

/**
 * A general purpose service for Object Relational Mappings that are persisted with Requery.
 */
class RequeryPersistenceService(override val schemaService: SchemaService,
                                val model: EntityModel) : AbstractPersistenceServiceImpl(schemaService) {

    private val configuration = RequeryConfiguration()

    companion object {
        val logger = loggerFor<RequeryPersistenceService>()
    }

    override fun persistStateWithSchema(state: QueryableState, stateRef: StateRef, schema: MappedSchema) {

        val session = configuration.sessionForModel(model)

        session.invoke {
            val mappedObject = schemaService.generateMappedObject(state, schema)
            mappedObject.stateRef = PersistentStateRef(stateRef)
            insert(mappedObject)
        }
    }

}