package net.corda.core.serialization.amqp

import com.google.common.primitives.Primitives
import org.apache.qpid.proton.amqp.*
import java.io.NotSerializableException
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.util.*
import java.util.concurrent.ConcurrentHashMap

// TODO: object references
// TODO: array support
// TODO: Set support?
// TODO: Do we support specific List, Map implementations or force all to interface
// TODO: Include some hash of the class properties into the text descriptor for a class
// TODO: Support java constructors via -parameters compiler option and/or an annotation (align with Jackson?)
// TODO: More generics scrutiny.  What about subclass of List with bound parameters, and/or another generic class?
// TODO: Write tests for boxed and unboxed primitives.
class SerializerFactory {
    private val serializersByType = ConcurrentHashMap<Type, Serializer>()
    private val serializersByDescriptor = ConcurrentHashMap<Any, Serializer>()

    @Throws(NotSerializableException::class)
    fun get(actualType: Class<*>?, declaredType: Type): Serializer {
        if (declaredType is ParameterizedType) {
            return serializersByType.computeIfAbsent(declaredType) {
                // We allow only List and Map.
                // TODO: support Set?
                val rawType = declaredType.rawType
                if (rawType is Class<*>) {
                    checkParameterisedTypesConcrete(declaredType.actualTypeArguments)
                    if (rawType.isAssignableFrom(List::class.java)) {
                        makeListSerializer(declaredType)
                    } else if (rawType.isAssignableFrom(Map::class.java)) {
                        makeMapSerializer(declaredType)
                    } else {
                        throw NotSerializableException("Declared types of $declaredType are not supported.")
                    }
                } else {
                    throw NotSerializableException("Declared types of $declaredType are not supported.")
                }
            }
        } else if (declaredType is Class<*>) {
            // Straight classes allowed
            return makeClassSerializer(actualType ?: declaredType)
        } else {
            throw NotSerializableException("Declared types of $declaredType are not supported.")
        }
    }

    @Throws(NotSerializableException::class)
    fun get(typeDescriptor: Any, envelope: Envelope): Serializer {
        return serializersByDescriptor[typeDescriptor] ?: {
            processSchema(envelope.schema)
            serializersByDescriptor[typeDescriptor] ?: throw NotSerializableException("Could not find type matching descriptor $typeDescriptor.")
        }()
    }

    private fun processSchema(schema: Schema) {
        for (typeNotation in schema.types) {
            processSchemaEntry(typeNotation)
        }
    }

    private fun processSchemaEntry(typeNotation: TypeNotation) {
        // TODO: use sealed types for TypeNotation?
        // TODO: for now we know the type is directly convertible, and we don't do any comparison etc etc
        if (typeNotation is CompositeType) {
            // java.lang.Class (whether a class or interface)
            processCompositeType(typeNotation)
        } else if (typeNotation is RestrictedType) {
            // List / Map, possibly with generics
            processRestrictedType(typeNotation)
        } else {
            throw NotSerializableException("Unexpected type $typeNotation")
        }
    }

    private fun processRestrictedType(typeNotation: RestrictedType) {
        serializersByDescriptor.computeIfAbsent(typeNotation.descriptor.name!!) {
            val type = DeserializedParameterizedType.make(typeNotation.name)
            get(null, type)
        }
    }

    private fun processCompositeType(typeNotation: CompositeType) {
        serializersByDescriptor.computeIfAbsent(typeNotation.descriptor.name!!) {
            val clazz = Class.forName(typeNotation.name)
            get(clazz, clazz)
        }
    }

    private fun checkParameterisedTypesConcrete(actualTypeArguments: Array<out Type>) {
        for (type in actualTypeArguments) {
            // Needs to be another parameterised type or a class
            if (type !is Class<*>) {
                if (type is ParameterizedType) {
                    checkParameterisedTypesConcrete(type.actualTypeArguments)
                } else {
                    throw NotSerializableException("Declared parameterised types containing $type as a parameter are not supported.")
                }
            }
        }
    }

    private fun makeClassSerializer(clazz: Class<*>): Serializer {
        return serializersByType.computeIfAbsent(clazz) {
            // TODO: check for array type
            if (isPrimitive(clazz)) {
                PrimitiveSerializer(clazz)
            } else {
                ClassSerializer(clazz)
            }
        }
    }

    private fun makeListSerializer(declaredType: ParameterizedType): Serializer {
        return ListSerializer(declaredType)
    }

    private fun makeMapSerializer(declaredType: ParameterizedType): Serializer {
        val rawType = declaredType.rawType as Class<*>
        if (HashMap::class.java.isAssignableFrom(rawType) && !LinkedHashMap::class.java.isAssignableFrom(rawType)) {
            throw NotSerializableException("Map type $declaredType is unstable under iteration.")
        }
        return MapSerializer(declaredType)
    }

    companion object {
        fun isPrimitive(type: Type): Boolean = type is Class<*> && Primitives.wrap(type) in primitiveTypeNames

        fun primitiveTypeName(type: Type): String? = primitiveTypeNames[type as? Class<*>]

        private val primitiveTypeNames: Map<Class<*>, String> = mapOf(Boolean::class.java to "boolean",
                Byte::class.java to "byte",
                UnsignedByte::class.java to "ubyte",
                Short::class.java to "short",
                UnsignedShort::class.java to "ushort",
                Integer::class.java to "int",
                UnsignedInteger::class.java to "uint",
                Long::class.java to "long",
                UnsignedLong::class.java to "ulong",
                Float::class.java to "float",
                Double::class.java to "double",
                Decimal32::class.java to "decimal32",
                Decimal64::class.java to "decimal62",
                Decimal128::class.java to "decimal128",
                Char::class.java to "char",
                Date::class.java to "timestamp",
                UUID::class.java to "uuid",
                ByteArray::class.java to "binary",
                String::class.java to "string",
                Symbol::class.java to "symbol")
    }
}