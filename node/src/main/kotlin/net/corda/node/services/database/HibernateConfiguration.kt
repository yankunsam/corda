package com.r3corda.node.services.database

import com.r3corda.node.services.schema.HibernatePersistenceService
import net.corda.core.schemas.MappedSchema
import net.corda.core.utilities.debug
import net.corda.core.utilities.loggerFor
import net.corda.node.services.api.SchemaService
import org.hibernate.SessionFactory
import org.hibernate.StatelessSession
import org.hibernate.boot.model.naming.Identifier
import org.hibernate.boot.model.naming.PhysicalNamingStrategyStandardImpl
import org.hibernate.cfg.Configuration
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment
import org.hibernate.service.UnknownUnwrapTypeException
import org.jetbrains.exposed.sql.transactions.TransactionManager
import java.sql.Connection
import java.util.concurrent.ConcurrentHashMap

class HibernateConfiguration(val schemaService: SchemaService) {

    companion object {
        val logger = loggerFor<HibernateConfiguration>()
    }

    // TODO: make this a guava cache or similar to limit ability for this to grow forever.
    private val sessionFactories = ConcurrentHashMap<MappedSchema, SessionFactory>()

    fun sessionFactoryForSchema(schema: MappedSchema): SessionFactory {
        return sessionFactories.computeIfAbsent(schema, { makeSessionFactoryForSchema(it) })
    }

    fun getSessionFactory(schema: MappedSchema): SessionFactory {
        return sessionFactoryForSchema(schema)
    }

    fun getSession(schema: MappedSchema): StatelessSession {
        val sessionFactory = sessionFactoryForSchema(schema)
        return sessionFactory.openStatelessSession(TransactionManager.current().connection)
    }

    protected fun makeSessionFactoryForSchema(schema: MappedSchema): SessionFactory {
        logger.info("Creating session factory for schema $schema")
        // We set a connection provider as the auto schema generation requires it.  The auto schema generation will not
        // necessarily remain and would likely be replaced by something like Liquibase.  For now it is very convenient though.
        // TODO: replace auto schema generation as it isn't intended for production use, according to Hibernate docs.
        val config = Configuration().setProperty("hibernate.connection.provider_class", HibernateConfiguration.NodeDatabaseConnectionProvider::class.java.name)
                .setProperty("hibernate.hbm2ddl.auto", "update")
                .setProperty("hibernate.show_sql", "false")
                .setProperty("hibernate.format_sql", "true")
//                .setProperty("hibernate.query.startup_check", "false")
        val options = schemaService.schemaOptions[schema]
        val databaseSchema = options?.databaseSchema
        if (databaseSchema != null) {
            logger.debug { "Database schema = $databaseSchema" }
            config.setProperty("hibernate.default_schema", databaseSchema)
        }
//        val tablePrefix = options?.tablePrefix ?: "contract_" // We always have this as the default for aesthetic reasons.
        val tablePrefix = ""
        logger.debug { "Table prefix = $tablePrefix" }
        config.setPhysicalNamingStrategy(object : PhysicalNamingStrategyStandardImpl() {
            override fun toPhysicalTableName(name: Identifier?, context: JdbcEnvironment?): Identifier {
                val default = super.toPhysicalTableName(name, context)
                return Identifier.toIdentifier(tablePrefix + default.text, default.isQuoted)
            }
        })
        schema.mappedTypes.forEach {
            config.addAnnotatedClass(it)
        }
        val sessionFactory = config.buildSessionFactory()
        HibernatePersistenceService.logger.info("Created session factory for schema $schema")
        return sessionFactory
    }

    // Supply Hibernate with connections from our underlying Exposed database integration.  Only used
    // during schema creation / update.
    class NodeDatabaseConnectionProvider : ConnectionProvider {
        override fun closeConnection(conn: Connection) {
            val tx = TransactionManager.current()
            tx.commit()
            tx.close()
        }

        override fun supportsAggressiveRelease(): Boolean = true

        override fun getConnection(): Connection {
            val tx = TransactionManager.manager.newTransaction(Connection.TRANSACTION_REPEATABLE_READ)
            return tx.connection
        }

        override fun <T : Any?> unwrap(unwrapType: Class<T>): T {
            try {
                return unwrapType.cast(this)
            } catch(e: ClassCastException) {
                throw UnknownUnwrapTypeException(unwrapType)
            }
        }

        override fun isUnwrappableAs(unwrapType: Class<*>?): Boolean = (unwrapType == NodeDatabaseConnectionProvider::class.java)
    }
}