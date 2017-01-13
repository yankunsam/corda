package com.r3corda.node.services.schema

import com.r3corda.node.services.database.HibernateConfiguration
import kotlinx.support.jdk7.use
import net.corda.core.contracts.StateRef
import net.corda.core.schemas.MappedSchema
import net.corda.core.schemas.PersistentStateRef
import net.corda.core.schemas.QueryableState
import net.corda.core.utilities.loggerFor
import net.corda.node.services.api.SchemaService
import net.corda.node.services.persistence.AbstractPersistenceServiceImpl
import org.jetbrains.exposed.sql.transactions.TransactionManager

/**
 * A general purpose service for Object Relational Mappings that are persisted with Hibernate.
 */
// TODO: Manage version evolution of the schemas via additional tooling.
class HibernatePersistenceService(override val schemaService: SchemaService) : AbstractPersistenceServiceImpl(schemaService) {

    private val configuration = HibernateConfiguration(schemaService)

    companion object {
        val logger = loggerFor<HibernatePersistenceService>()
    }

    override fun persistStateWithSchema(state: QueryableState, stateRef: StateRef, schema: MappedSchema) {
        val sessionFactory = configuration.sessionFactoryForSchema(schema)
        val session = sessionFactory.openStatelessSession(TransactionManager.current().connection)
        session.use {
            val mappedObject = schemaService.generateMappedObject(state, schema)
            mappedObject.stateRef = PersistentStateRef(stateRef)
            session.insert(mappedObject)
        }
    }
}

