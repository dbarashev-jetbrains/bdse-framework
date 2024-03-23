package kvas.util

import io.grpc.ManagedChannelBuilder
import kvas.proto.KvasGrpc
import kvas.proto.KvasGrpc.KvasBlockingStub
import kvas.proto.KvasProto.LogEntryNumber
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

fun String.toHostPort(defaultPort: Int = 9000) = this.split(':').let {
    if (it.size == 2) {
        it[0] to it[1].toInt()
    } else {
        it[0] to defaultPort
    }
}

class KvasPool {
    private val stubs = mutableMapOf<String, KvasBlockingStub>()
    fun <T> kvas(address: String, code: KvasBlockingStub.()->T) =
        code(stubs.getOrPut(address) {
            address.toHostPort().let {kvas(it.first, it.second)}
        })

    val nodes: Map<String, KvasBlockingStub> get() = stubs.toMap()

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