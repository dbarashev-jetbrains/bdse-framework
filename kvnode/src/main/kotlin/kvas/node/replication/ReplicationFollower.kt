package kvas.node.replication

import kvas.node.storage.RowScan
import kvas.node.storage.Storage
import kvas.node.storage.StoredRow
import kvas.proto.KvasReplicationProto
import kvas.proto.MetadataServiceGrpc.MetadataServiceBlockingStub
import kvas.proto.ReplicationFollowerGrpcKt
import kvas.proto.appendLogResponse
import kvas.util.NodeAddress

interface ReplicationFollower {
    val storage: Storage
    val createGrpcService: () -> ReplicationFollowerGrpcKt.ReplicationFollowerCoroutineImplBase
}

class NaiveFollowerStorage(private val storageDelegate: Storage) : Storage {
    override fun put(rowKey: String, columnName: String, value: String) {
        error("I am read-only, bro")
    }

    override fun get(rowKey: String, columnName: String): String? {
        return storageDelegate.get(rowKey, columnName)
    }

    override fun getRow(rowKey: String): StoredRow? {
        return storageDelegate.getRow(rowKey)
    }

    override fun scan(conditions: Map<String, String>): RowScan {
        return storageDelegate.scan(conditions)
    }
}

class NaiveReplicationFollower(private val storageDelegate: Storage) :
    ReplicationFollowerGrpcKt.ReplicationFollowerCoroutineImplBase(), ReplicationFollower {
    override suspend fun appendLog(request: KvasReplicationProto.AppendLogRequest): KvasReplicationProto.AppendLogResponse {
        request.entriesList.forEach {
            it.dataRow.valuesMap.forEach { (k, v) -> storageDelegate.put(it.dataRow.key, k, v) }
        }
        return appendLogResponse { }
    }

    override val storage = NaiveFollowerStorage(storageDelegate)
    override val createGrpcService: () -> ReplicationFollowerGrpcKt.ReplicationFollowerCoroutineImplBase
        get() = { this }
}

/**
 * Holds all replication follower implementations.
 */
object ReplicationFollowerFactory {
    val NAIVE = "naive" to ::createNaiveReplicationFollower
    val ALL = listOf(NAIVE).toMap()
}

fun createNaiveReplicationFollower(
    selfAddress: NodeAddress,
    storage: Storage,
    metadataStub: MetadataServiceBlockingStub
): NaiveReplicationFollower = NaiveReplicationFollower(storage)

