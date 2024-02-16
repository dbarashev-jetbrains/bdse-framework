package kvas.util

import io.grpc.ManagedChannelBuilder
import kvas.proto.KvasGrpc

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

