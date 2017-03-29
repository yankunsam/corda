package net.corda.client.rpc

import net.corda.core.flatMap
import net.corda.core.messaging.RPCOps
import net.corda.nodeapi.User
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(Parameterized::class)
open class AbstractRpcTest {
    enum class RpcTestMode {
        InVm,
        Netty,
        Node
    }

    companion object {
        @JvmStatic @Parameterized.Parameters(name = "Mode = {0}")
        fun data(): Collection<Array<out Any>> {
            return listOf(RpcTestMode.InVm, RpcTestMode.Netty).map { arrayOf(it) }
        }
    }
    @Parameterized.Parameter
    lateinit var mode: RpcTestMode

    inline fun <reified I : RPCOps> RpcExposedDSLInterface.testProxy(ops: I, rpcUser: User = rpcTestUser): I {
        return when (mode) {
            RpcTestMode.InVm ->
                startInVmRpcServer(ops = ops, rpcUser = rpcUser).flatMap {
                    startInVmRpcClient<I>(rpcUser.username, rpcUser.password)
                }.get()
            RpcTestMode.Netty ->
                startRpcServer(ops = ops, rpcUser = rpcUser).flatMap {
                    startRpcClient<I>(it.hostAndPort, rpcUser.username, rpcUser.password)
                }.get()
            RpcTestMode.Node ->
                startNode(rpcUsers = listOf(rpcUser)).flatMap {
                    startRpcClient<I>(it.configuration.rpcAddress!!, rpcUser.username, rpcUser.password)
                }.get()
        }
    }
}
