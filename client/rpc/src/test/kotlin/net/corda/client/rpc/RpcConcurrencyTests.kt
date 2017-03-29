package net.corda.client.rpc

import net.corda.core.future
import net.corda.core.messaging.RPCOps
import net.corda.core.random63BitValue
import org.junit.Test
import java.io.InputStream
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import kotlin.test.assertEquals


class RpcConcurrencyTests : AbstractRpcTest() {

    private interface TestOps : RPCOps {
        fun newLatch(numberOfDowns: Int): Long
        fun waitLatch(id: Long)
        fun downLatch(id: Long)

        fun echoStream(stream: InputStream): InputStream
    }

    class TestOpsImpl : TestOps {
        private val latches = ConcurrentHashMap<Long, CountDownLatch>()
        override val protocolVersion = 0

        override fun newLatch(numberOfDowns: Int): Long {
            val id = random63BitValue()
            val latch = CountDownLatch(numberOfDowns)
            latches.put(id, latch)
            return id
        }

        override fun waitLatch(id: Long) {
            latches[id]!!.await()
        }

        override fun downLatch(id: Long) {
            latches[id]!!.countDown()
        }

        override fun echoStream(stream: InputStream): InputStream {
            return object : InputStream() {
                var overall = 0
                override fun read(): Int {
                    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
                }

                override fun read(p0: ByteArray?, p1: Int, p2: Int): Int {
                    val byteNumber = stream.read(p0, p1, p2)
                    overall += byteNumber
                    println("Overall $overall")
                    return byteNumber
                }
            }
        }
    }

    private fun RpcExposedDSLInterface.testProxy() = testProxy<TestOps>(TestOpsImpl())

    @Test
    fun `call multiple RPCs in parallel`() {
        rpcDriver {
            val proxy = testProxy()
            val N = 1
            val M = 10
            val id = proxy.newLatch(M)
            val done = CountDownLatch(N)
            (1 .. N).forEach {
                future {
                    println("Waiting $it")
                    proxy.waitLatch(id)
                    println("Waiting $it done")
                    done.countDown()
                }
            }
            (1 .. M).toList().parallelStream().forEach {
                println("Downing $it")
                proxy.downLatch(id)
                println("Downing $it done")
            }
            done.await()
        }
    }

    @Test
    fun `large message stream`() {
        rpcDriver {
            val size = 4 * 1024 * 1024
            val proxy = testProxy()
            val stream = RepeatingBytesInputStream(byteArrayOf(1, 2), size)
            println("Streaming up")
            val reply = proxy.echoStream(stream)
            println("Streaming down")
            val replyBytes = reply.readBytes()
            assertEquals(size, replyBytes.size)
        }

    }
}