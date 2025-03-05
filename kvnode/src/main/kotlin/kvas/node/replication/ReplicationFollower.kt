package kvas.node.replication

import com.google.protobuf.StringValue
import kvas.node.storage.RowScan
import kvas.node.storage.Storage
import kvas.node.storage.StoredRow
import kvas.proto.*
import kvas.proto.MetadataServiceGrpc.MetadataServiceBlockingStub
import kvas.util.NodeAddress

interface ReplicationFollower {
    val storage: Storage
    val createGrpcService: () -> ReplicationFollowerGrpcKt.ReplicationFollowerCoroutineImplBase
}

class DemoFollowerStorage(private val storageDelegate: Storage) : Storage {
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

class DemoReplicationFollower(private val storageDelegate: Storage) :
    ReplicationFollowerGrpcKt.ReplicationFollowerCoroutineImplBase(), ReplicationFollower {
    override suspend fun appendLog(request: KvasReplicationProto.AppendLogRequest): KvasReplicationProto.AppendLogResponse {
        request.entriesList.forEach {
            it.dataRow.valuesMap.forEach { (k, v) -> storageDelegate.put(it.dataRow.key, k, v) }
        }
        return appendLogResponse { }
    }

    override suspend fun getValue(request: KvasProto.GetValueRequest): KvasProto.GetValueResponse {
        val row = storageDelegate.getRow(request.rowKey)
        val values = row?.valuesMap
        return values?.getValue(request.columnName)?.let {
            getValueResponse {
                version = row.version
                value = StringValue.of(it)
                code = KvasProto.GetValueResponse.StatusCode.OK
            }
        } ?: getValueResponse {
            code = KvasProto.GetValueResponse.StatusCode.OK
        }
    }

    override val storage = DemoFollowerStorage(storageDelegate)
    override val createGrpcService: () -> ReplicationFollowerGrpcKt.ReplicationFollowerCoroutineImplBase
        get() = { this }
}

/**
 * Holds all replication follower implementations.
 */
object ReplicationFollowerFactory {
    val NAIVE = "demo" to ::createDemoReplicationFollower
    val ALL = listOf(NAIVE).toMap()
}

fun createDemoReplicationFollower(
    selfAddress: NodeAddress,
    storage: Storage,
    metadataStub: MetadataServiceBlockingStub
): DemoReplicationFollower = DemoReplicationFollower(storage)

