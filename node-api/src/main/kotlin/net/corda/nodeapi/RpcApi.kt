package net.corda.nodeapi

import com.esotericsoftware.kryo.pool.KryoPool
import net.corda.core.ErrorOr
import net.corda.core.serialization.*
import net.corda.core.utilities.loggerFor
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.api.core.client.ClientMessage
import org.apache.activemq.artemis.reader.MessageUtil
import rx.Notification
import java.io.*
import java.util.*
import java.util.concurrent.ExecutorService

val log = loggerFor<RpcApi>()

fun ClientMessage.streamBody(executor: ExecutorService): InputStream {
//    return ByteArrayInputStream(this.bodyBuffer.byteBuf().array())
    val outputStream = PipedOutputStream()
    val inputStream = PipedInputStream(outputStream, 102400)
    executor.submit {
        setOutputStream(outputStream)
    }
    return inputStream
}

fun <T : Any> T.streamToClientMessageBody(executor: ExecutorService, kryoPool: KryoPool, message: ClientMessage) {
//    message.bodyBuffer.writeBytes(serialize(kryoPool).bytes)
//    return
    val kryoOutputStream = PipedOutputStream()
    val kryoInputStream = PipedInputStream(kryoOutputStream, 102400)
    val inputStreams = Vector<InputStream>()
    inputStreams.addElement(kryoInputStream)
    message.setBodyInputStream(SequenceInputStream(inputStreams.elements()))
    executor.submit {
        try {
            kryoPool.run { kryo ->
                serializeToStream(kryo, kryoOutputStream)
                val inputStreamToWrite = kryo.context.remove(InputStreamSerializer.InputStreamToWriteKey) as InputStream?
                if (inputStreamToWrite != null) {
                    inputStreams.addElement(inputStreamToWrite)
                }
                kryoOutputStream.close()
            }
        } catch(t : Throwable) {
            println("ehh $t")
            t.printStackTrace()
            throw t
        }
    }
}

object RpcApi {
    private val TAG_FIELD_NAME = "tag"
    private val RPC_ID_FIELD_NAME = "rpc-id"
    private val OBSERVABLE_ID_FIELD_NAME = "observable-id"
    private val METHOD_NAME_FIELD_NAME = "method-name"

    val RPC_SERVER_QUEUE_NAME = "rpc-server"
    val RPC_CLIENT_QUEUE_NAME_PREFIX = "rpc-client"

    data class RpcRequestId(val toLong: Long)
    data class ObservableId(val toLong: Long)

    sealed class ClientToServer {
        private enum class Tag {
            RPC_REQUEST,
            OBSERVABLES_CLOSED
        }

        abstract fun <A> accept(
                onRpcRequest: (RpcRequest) -> A,
                onObservablesClosed: (ObservablesClosed) -> A
        ): A

        data class RpcRequest(
                val clientAddress: SimpleString,
                val id: RpcRequestId,
                val methodName: String,
                val arguments: List<Any?>
        ) : ClientToServer() {
            override fun <A> accept(onRpcRequest: (RpcRequest) -> A, onObservablesClosed: (ObservablesClosed) -> A): A {
                return onRpcRequest(this)
            }

            fun writeToClientMessage(executor: ExecutorService, kryo: KryoPool, message: ClientMessage) {
                MessageUtil.setJMSReplyTo(message, clientAddress)
                message.putIntProperty(TAG_FIELD_NAME, Tag.RPC_REQUEST.ordinal)
                message.putLongProperty(RPC_ID_FIELD_NAME, id.toLong)
                message.putStringProperty(METHOD_NAME_FIELD_NAME, methodName)
                arguments.streamToClientMessageBody(executor, kryo, message)
            }
        }

        data class ObservablesClosed(
                val ids: List<ObservableId>
        ) : ClientToServer() {
            override fun <A> accept(onRpcRequest: (RpcRequest) -> A, onObservablesClosed: (ObservablesClosed) -> A): A {
                return onObservablesClosed(this)
            }

            fun writeToClientMessage(message: ClientMessage) {
                message.putIntProperty(TAG_FIELD_NAME, Tag.OBSERVABLES_CLOSED.ordinal)
                val buffer = message.bodyBuffer
                buffer.writeInt(ids.size)
                ids.forEach {
                    buffer.writeLong(it.toLong)
                }
            }
        }

        companion object {
            fun fromClientMessage(executor: ExecutorService, kryoPool: KryoPool, message: ClientMessage): ClientToServer {
                val tag = Tag.values()[message.getIntProperty(TAG_FIELD_NAME)]
                return when (tag) {
                    RpcApi.ClientToServer.Tag.RPC_REQUEST -> RpcRequest(
                            clientAddress = MessageUtil.getJMSReplyTo(message),
                            id = RpcRequestId(message.getLongProperty(RPC_ID_FIELD_NAME)),
                            methodName = message.getStringProperty(METHOD_NAME_FIELD_NAME),
                            arguments = message.streamBody(executor).deserializeAndSetLeftoverPossibly(kryoPool)
                    )
                    RpcApi.ClientToServer.Tag.OBSERVABLES_CLOSED -> {
                        val ids = ArrayList<ObservableId>()
                        val buffer = message.bodyBuffer
                        val numberOfIds = buffer.readInt()
                        for (i in 1 .. numberOfIds) {
                            ids.add(ObservableId(buffer.readLong()))
                        }
                        ObservablesClosed(ids)
                    }
                }
            }
        }
    }

    sealed class ServerToClient {
        private enum class Tag {
            RPC_REPLY,
            OBSERVATION
        }

        abstract fun <A> accept(
                onRpcReply: (RpcReply) -> A,
                onObservation: (Observation) -> A
        ): A

        abstract fun writeToClientMessage(executor: ExecutorService, kryoPool: KryoPool, message: ClientMessage)

        data class RpcReply(
                val id: RpcRequestId,
                val result: ErrorOr<Any?>
        ) : ServerToClient() {
            override fun <A> accept(onRpcReply: (RpcReply) -> A, onObservation: (Observation) -> A): A {
                return onRpcReply(this)
            }

            override fun writeToClientMessage(executor: ExecutorService, kryoPool: KryoPool, message: ClientMessage) {
                message.putIntProperty(TAG_FIELD_NAME, Tag.RPC_REPLY.ordinal)
                message.putLongProperty(RPC_ID_FIELD_NAME, id.toLong)
                result.streamToClientMessageBody(executor, kryoPool, message)
            }
        }

        data class Observation(
                val id: ObservableId,
                val content: Notification<Any>
        ) : ServerToClient() {
            override fun <A> accept(onRpcReply: (RpcReply) -> A, onObservation: (Observation) -> A): A {
                return onObservation(this)
            }

            override fun writeToClientMessage(executor: ExecutorService, kryoPool: KryoPool, message: ClientMessage) {
                message.putIntProperty(TAG_FIELD_NAME, Tag.OBSERVATION.ordinal)
                message.putLongProperty(OBSERVABLE_ID_FIELD_NAME, id.toLong)
                content.streamToClientMessageBody(executor, kryoPool, message)
            }
        }

        companion object {
            fun fromClientMessage(executor: ExecutorService, kryoPool: KryoPool, message: ClientMessage): ServerToClient {
                val tag = Tag.values()[message.getIntProperty(TAG_FIELD_NAME)]
                return when (tag) {
                    RpcApi.ServerToClient.Tag.RPC_REPLY -> RpcReply(
                            id = RpcRequestId(message.getLongProperty(RPC_ID_FIELD_NAME)),
                            result = message.streamBody(executor).deserializeAndSetLeftoverPossibly(kryoPool)
                    )
                    RpcApi.ServerToClient.Tag.OBSERVATION -> Observation(
                            id = ObservableId(message.getLongProperty(OBSERVABLE_ID_FIELD_NAME)),
                            content = message.streamBody(executor).deserializeAndSetLeftoverPossibly(kryoPool)
                    )
                }
            }
        }
    }
}