package com.r3corda.node.services.query

import com.r3corda.node.services.database.HibernateConfiguration
import io.requery.Persistable
import kotlinx.support.jdk7.use
import net.corda.core.node.services.QueryService
import net.corda.core.schemas.MappedSchema
import net.corda.core.utilities.loggerFor
import net.corda.node.services.api.SchemaService
import javax.persistence.Query
import javax.persistence.TypedQuery
import javax.persistence.criteria.CriteriaBuilder
import javax.persistence.criteria.CriteriaQuery
import javax.persistence.metamodel.EntityType
import kotlin.reflect.KClass

/**
 * A general purpose Query service for Object Relational query using Hibernate
 */
class HibernateQueryServiceImpl(val schemaService: SchemaService) : QueryService  {

    companion object {
        val logger = loggerFor<HibernateQueryServiceImpl>()
    }

    val configuration = HibernateConfiguration(schemaService)

    /**
     *  The Hibernate Query Language (HQL) and Java Persistence Query Language (JPQL) are both object model
     *  focused query languages similar in nature to SQL.
     *  Both HQL and JPQL are non-type-safe ways to perform query operations.
     *
     *  See https://docs.jboss.org/hibernate/orm/current/userguide/html_single/chapters/query/hql/HQL.html
     */

    /**
     *  Use to pass in dynamic queries (eg. queries defined directly within your apps business logic)
     *
     *  Two forms:
     *  1) Using Named Parameters
     *     SELECT c FROM Cash c WHERE c.currency = :currencyName
     *     In this form use [args] to pass in a sequence of Pair[NamedParam, NamedValue]
     *  2) Using Positional Parameters
     *     SELECT c FROM Cash c WHERE c.currency = ?1
     *     In this form use [args] to pass in an ordered sequence of NamedValue(s)
     *
     *  Example Queries can be found at <http://docs.oracle.com/javaee/6/tutorial/doc/bnbtl.html>
     *
     *  Full Query Language syntax can be found at <http://docs.oracle.com/javaee/6/tutorial/doc/bnbuf.html>
     */
    override fun simpleQueryForSchema(sqlString: String, schema: MappedSchema, vararg args: Any?): Iterable<Any?> {
        configuration.getSession(schema).use { s ->
            val query = constructQuery(s.createQuery(sqlString), args)
            return query.resultList
        }
    }

    override fun simpleQueryForSchemaUsingNamedArgs(sqlString: String, schema: MappedSchema, vararg args: String): Iterable<Any?> {
        configuration.getSession(schema).use { s ->
            val query = constructQueryUsingNamedArgs(s.createQuery(sqlString), args)
            return query.resultList
        }
    }

    /**
     * Use to call static queries that are defined in the [ContractState] Schema using NamedQuery annotations
     * Pass in the NamedQuery name, and associated parameters (Named or Positional) using [args] as described above.*
     */
    override fun namedQueryForSchema(queryName: String, schema: MappedSchema, vararg args: Any?): Iterable<Any?> {
        configuration.getSession(schema).use { s ->
            val query = constructQuery(s.createNamedQuery(queryName), args)
            val results = query.resultList
            return results
        }
    }

    /**
     *  Use to express queries in the native SQL dialect of your underlying database
     *  See <https://docs.jboss.org/hibernate/orm/current/userguide/html_single/chapters/query/native/Native.html>
     */
    override fun nativeQueryForSchema(sqlString: String, schema: MappedSchema, vararg args: Any?): Iterable<Any?> {
        configuration.getSession(schema).use { s ->
            val query = constructQuery(s.createNativeQuery(sqlString), args)
            return query.resultList.asIterable()
        }
    }

    /**
     *  Criteria queries offer a type-safe alternative to HQL, JPQL and native SQL queries.
     *  See <https://docs.jboss.org/hibernate/orm/current/userguide/html_single/chapters/query/criteria/Criteria.html>
     *
     *  Criteria query using Metamodel
     *  See <http://docs.oracle.com/javaee/6/tutorial/doc/gjiup.html>
     */
     override fun <T : Persistable> criteriaQueryForSchema(schema: MappedSchema, entityClass: KClass<T>, vararg args: String?): List<T> {
        /*
         * sample usage: SELECT t FROM <T> t
         */
        configuration.getSessionFactory(schema).use { sf ->
            val em = sf.createEntityManager()
            val builder = em.criteriaBuilder
            val criteria = constructCriteria(builder, entityClass.java, args)
            val query: TypedQuery<T> = em.createQuery(criteria)
            val results: List<T> = query.resultList
            return results
        }
    }

    private fun <T> constructCriteria(builder: CriteriaBuilder, entityClass: Class<T>, args: Array<out String?>): CriteriaQuery<T> {
        val criteria = builder.createQuery(entityClass)
        val root = criteria.from(entityClass)
        val meta: EntityType<T> = root.model
        criteria.select(root)
        for (i in args.indices step 2) {
            criteria.where(builder.equal(root.get(meta.getSingularAttribute(args[i])), args[i+1]))
        }
        return criteria
    }

    /**
     *  Query Parameters which should be defaulted (or custom set)
     *      FirstResults (eg. pageId * pageSize)
     *      MaxResults (eg. pageSize)
     *      FlushMode (eg. AUTO(def) or COMMIT)
     *      LockMode (eg. OPTIMISTIC, etc)
     *      Hibernate specific query hints:
     *          Query Timeout
     *          Result-fetch mode (EAGER, LAZY)
     *
     *  args are interpreted as Ordinal Parameters (?index) for Query
     */
    private fun constructQuery(query: Query, args: Array<out Any?>): Query {
        var index = 1
        args.forEach {  query.setParameter(index++, it)  }
        return query
    }

    private fun constructQueryUsingNamedArgs(query: Query, args: Array<out String>): Query {
        require(args.size % 2 == 0 ) { "Query argument mismatch: expecting [<argName>,<argValue>] pairs. Processed $args" }
        for (i in args.indices step 2) {
            query.setParameter(args[i], args[i+1])
        }
        return query
    }
}
