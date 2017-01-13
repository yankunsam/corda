package net.corda.node.services.query

import com.r3corda.node.services.database.RequeryConfiguration
import io.requery.Persistable
import io.requery.kotlin.QueryableAttribute
import io.requery.meta.EntityModel
import net.corda.core.node.services.QueryService
import net.corda.core.schemas.MappedSchema
import net.corda.core.utilities.loggerFor
import net.corda.node.services.api.SchemaService
import kotlin.reflect.KClass

/**
 * A general purpose Query service using Requery (DSL based query that maps to SQL)
 */
class RequeryQueryServiceImpl(val schemaService: SchemaService,
                              val model: EntityModel) : QueryService {

    companion object {
        val logger = loggerFor<RequeryQueryServiceImpl>()
    }

    val configuration = RequeryConfiguration()

    override fun simpleQueryForSchema(sqlString: String, schema: MappedSchema, vararg args: Any?): Iterable<Any?> {
        throw UnsupportedOperationException("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun simpleQueryForSchemaUsingNamedArgs(sqlString: String, schema: MappedSchema, vararg args: String): Iterable<Any?> {
        throw UnsupportedOperationException("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun nativeQueryForSchema(sqlString: String, schema: MappedSchema, vararg args: Any?): Iterable<Any?> {

        val attributes: QueryableAttribute<Any, *>? = convertToQueryableAttributes(args)

        val results =
            attributes?.let {
                        configuration.sessionForModel(model).
                                select(attributes).get().asIterable()
            }

        return results ?: emptyList()
    }

    private fun convertToQueryableAttributes(args: Array<out Any?>): QueryableAttribute<Any, *>? {
        println(args)
        return null
    }

    override fun namedQueryForSchema(queryName: String, schema: MappedSchema, vararg args: Any?): Iterable<Any?> {
        throw UnsupportedOperationException("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <T : Persistable> criteriaQueryForSchema(schema: MappedSchema, entityClass: KClass<T>, vararg args: String?): List<T> {
        /*
         * sample usage: SELECT t FROM <T> t
         */
        val results =
            configuration.sessionForModel(model).invoke {
                val result = select(entityClass)
                result.get().toList()
            }
        return results
    }
}


