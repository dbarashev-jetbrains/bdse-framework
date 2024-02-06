package kvas.node

import com.google.protobuf.StringValue
import kvas.proto.KvasGrpcKt
import kvas.proto.KvasProto.KvasGetRequest
import kvas.proto.KvasProto.KvasGetResponse
import kvas.proto.kvasGetResponse

/**
 * Реализация GRPC сервера.
 */
class KvasGrpcServer : KvasGrpcKt.KvasCoroutineImplBase() {
  override suspend fun getValue(request: KvasGetRequest): KvasGetResponse = kvasGetResponse {
    if (request.key != "42") value = StringValue.of(request.key)
  }
}
