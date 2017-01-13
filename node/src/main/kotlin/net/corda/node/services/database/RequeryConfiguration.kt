package com.r3corda.node.services.database

import com.zaxxer.hikari.HikariConfig

import com.zaxxer.hikari.HikariDataSource
import io.requery.Persistable
import io.requery.meta.EntityModel
import io.requery.sql.KotlinConfiguration
import io.requery.sql.KotlinEntityDataStore
import io.requery.sql.SchemaModifier
import io.requery.sql.TableCreationMode
import net.corda.core.crypto.SecureHash
import net.corda.core.utilities.loggerFor
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class RequeryConfiguration() {

    companion object {
        val logger = loggerFor<RequeryConfiguration>()
    }

    // TODO:
    // 1. schemaService schemaOptions needs to be applied: eg. default schema, table prefix
    // 2. set other generic database configuration options: show_sql, format_sql
    // 3. Configure Requery Database platform specific features (see http://requery.github.io/javadoc/io/requery/sql/Platform.html)
    // 4. Configure Cache Manager and Cache Provider and set in Requery Configuration (see http://requery.github.io/javadoc/io/requery/EntityCache.html)

    // Note: Annotations are pre-processed using (kapt) so no need to register dynamically
    val config = HikariConfig(configureDataSourceProperties())

    val dataSource = HikariDataSource(config)

    // TODO: make this a guava cache or similar to limit ability for this to grow forever.
    private val sessionFactories = ConcurrentHashMap<EntityModel, KotlinEntityDataStore<Persistable>>()

    fun sessionForModel(model: EntityModel): KotlinEntityDataStore<Persistable> {
        return sessionFactories.computeIfAbsent(model, { makeSessionFactoryForModel(it) })
    }

    fun makeSessionFactoryForModel(model: EntityModel): KotlinEntityDataStore<Persistable> {
        val configuration = KotlinConfiguration(model, dataSource, useDefaultLogging = true)
        val tables = SchemaModifier(configuration)
        val mode = TableCreationMode.CREATE_NOT_EXISTS
        tables.createTables(mode)
        return KotlinEntityDataStore<Persistable>(configuration)
    }

    /**
     * Make properties appropriate for creating a DataSource
     *
     * @param nodeName Reflects an instance of the in-memory database.  Defaults to a random string.
     */
    private fun configureDataSourceProperties(nodeName: String = SecureHash.randomSHA256().toString()): Properties {
        val props = Properties()
        props.setProperty("dataSourceClassName", "org.h2.jdbcx.JdbcDataSource")
        props.setProperty("dataSource.url", "jdbc:h2:mem:corda_${nodeName};TRACE_LEVEL_SYSTEM_OUT=2;DB_CLOSE_ON_EXIT=FALSE")
        props.setProperty("dataSource.user", "sa")
        props.setProperty("dataSource.password", "")
        return props
    }
}