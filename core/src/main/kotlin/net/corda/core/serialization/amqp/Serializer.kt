package net.corda.core.serialization.amqp

import org.apache.qpid.proton.codec.Data
import java.lang.reflect.Type

/**
 * Implemented to serialize and deserialize different types of objects.
 */
interface Serializer {
    /**
     * The JVM type this can serialize and deserialize.
     */
    val type: Type

    /**
     * Textual unique representation of the JVM type this represents.  Will be encoded into the AMQP stream and
     * will appear in the schema.
     *
     * Typically this is unique enough that we can use one global cache of [Serializer]s and use this as the look up key.
     */
    val typeDescriptor: String

    /**
     * Add anything required to the AMQP schema via [SerializationOutput.writeTypeNotations] and any dependent serializers
     * via [SerializationOutput.requireSerializer]. e.g. for the elements of an array.
     */
    fun writeClassInfo(output: SerializationOutput)

    /**
     * Write the given object, with declared type, to the output.
     */
    fun writeObject(obj: Any, data: Data, type: Type, output: SerializationOutput)

    /**
     * Read the given object from the input. The envelope is provided in case the schema is required.
     */
    fun readObject(obj: Any, envelope: Envelope, input: DeserializationInput): Any
}