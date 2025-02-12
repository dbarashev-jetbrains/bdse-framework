package kvas.node

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kvas.node.storage.Storage
import kvas.proto.DataTransferServiceGrpcKt
import kvas.proto.KvasDataTransferProto
import kvas.proto.KvasSharedProto.DataRow
import kvas.proto.finishDataTransferResponse
import kvas.proto.initiateDataTransferResponse

object DataTransferServices {
    val DEMO = "demo" to ::DummyDataTransferService
    val REAL = "real" to { TODO("Task2: implement a Data Transfer Service to allow for re-sharding") }
    val ALL = listOf(DEMO).toMap()
}


/**
 * This is a dummy implementation of a data transfer service that just emits all values from the storage to any node that
 * requests them.
 */
class DummyDataTransferService(private val storage: Storage) :
    DataTransferServiceGrpcKt.DataTransferServiceCoroutineImplBase() {
    override suspend fun initiateDataTransfer(request: KvasDataTransferProto.InitiateDataTransferRequest) =
        initiateDataTransferResponse {
            this.transferId = -1
        }

    override fun startDataTransfer(request: KvasDataTransferProto.StartDataTransferRequest): Flow<DataRow> {
        return storage.scan().asFlow().map { DataRow.newBuilder().setKey(it.key).putAllValues(it.valuesMap).build() }
    }

    override suspend fun finishDataTransfer(request: KvasDataTransferProto.FinishDataTransferRequest) =
        finishDataTransferResponse {
        }
}