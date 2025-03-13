package kvas.util

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.AbstractBlockingStub
import kvas.proto.KvasGrpc
import kvas.proto.KvasReplicationProto.LogEntryNumber
import kotlin.math.sign

/**
 * Creates a blocking GRPC stub for the given Kvas server host and port
 */
fun kvas(host: String, port: Int) = KvasGrpc.newBlockingStub(
    ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
)

fun kvasync(host: String, port: Int) = KvasGrpc.newFutureStub(
    ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
)

data class NodeAddress(val host: String, val port: Int, var lastHeartbeat: Long = System.currentTimeMillis()) {
    override fun toString(): String {
        return "$host:$port"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as NodeAddress

        if (port != other.port) return false
        if (host != other.host) return false

        return true
    }

    override fun hashCode(): Int {
        var result = port
        result = 31 * result + host.hashCode()
        return result
    }

}

fun String.toNodeAddress() = this.toHostPort().let {
    NodeAddress(it.first, it.second)
}

fun String.toHostPort(defaultPort: Int = 9000) = this.split(':').let {
    if (it.size == 2) {
        it[0] to it[1].toInt()
    } else {
        it[0] to defaultPort
    }
}

interface GrpcPool<Stub : AbstractBlockingStub<Stub>> : AutoCloseable {
    fun <T> rpc(address: NodeAddress, code: Stub.() -> T): T
}

class KvasPool<Stub : AbstractBlockingStub<Stub>>(
    private val selfAddress: NodeAddress,
    private val stubFactory: (NodeAddress) -> Stub
) : GrpcPool<Stub> {
    var isNodeOffline = false
        set(value) {
            println(
                """
                ---------------------------------------
                This node is going ${if (value) "OFFLINE" else "ONLINE"}
                ---------------------------------------
                """.trimIndent()
            )
            field = value
        }
    private val stubs = mutableMapOf<NodeAddress, Stub>()

    override fun <T> rpc(address: NodeAddress, code: Stub.() -> T) =
        if (address != selfAddress && isNodeOffline) throw RuntimeException("This node is currently offline")
        else code(stubs.getOrPut(address) { stubFactory(address).withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS) })

    val nodes: Map<NodeAddress, Stub> get() = stubs.toMap()

    override fun close() {
        stubs.values.forEach { (it.channel as? ManagedChannel)?.shutdownNow() }
    }
}

fun LogEntryNumber.compareTo(other: LogEntryNumber) =
    (this.termNumber - other.termNumber).let {
        when {
            it < 0 -> -1
            it > 0 -> 1
            else -> (this.ordinalNumber - other.ordinalNumber).sign
        }
    }

fun LogEntryNumber.toLogString() = "$termNumber:$ordinalNumber"
data class NodeStatistics(val address: NodeAddress, val readSuccessRate: Double, val writeSuccessRate: Double, val readTotal: Int, val writeTotal: Int)
