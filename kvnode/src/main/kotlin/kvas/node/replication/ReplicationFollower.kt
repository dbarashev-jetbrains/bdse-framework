package kvas.node.replication

import kvas.node.storage.RowScan
import kvas.node.storage.Storage
import kvas.node.storage.StoredRow
import kvas.proto.KvasReplicationProto
import kvas.proto.MetadataServiceGrpc.MetadataServiceBlockingStub
import kvas.proto.ReplicationFollowerGrpcKt
import kvas.proto.appendLogResponse
import kvas.util.NodeAddress

/**
 * Represents a follower in a leader-based replication system, responsible for maintaining a replica of data.
 *
 */
interface ReplicationFollower {
    /**
     * Returns a follower's storage instance which is used in a DataService implementation.
     */
    val storage: Storage

    /**
     * Creates the follower's gRPC service that provides AppendLog implementation.
     */
    val createGrpcService: () -> ReplicationFollowerGrpcKt.ReplicationFollowerCoroutineImplBase
}

/**
 * Follower's storage implementation for demo purposes. It rejects PUT requests and delegates GET requests to
 * the real storage.
 */
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

/**
 * Demo implementation of the replication follower. It just scans through the log entries and applies them to the storage
 * immediately.
 */
class DemoReplicationFollower(private val storageDelegate: Storage) :
    ReplicationFollowerGrpcKt.ReplicationFollowerCoroutineImplBase(), ReplicationFollower {
    override suspend fun appendLog(request: KvasReplicationProto.AppendLogRequest): KvasReplicationProto.AppendLogResponse {
        request.entriesList.forEach {
            it.dataRow.valuesMap.forEach { (k, v) -> storageDelegate.put(it.dataRow.key, k, v) }
        }
        return appendLogResponse { }
    }

    override val storage = DemoFollowerStorage(storageDelegate)
    override val createGrpcService: () -> ReplicationFollowerGrpcKt.ReplicationFollowerCoroutineImplBase
        get() = { this }
}

typealias ReplicationFollowerFactory = (NodeAddress, Storage, MetadataServiceBlockingStub) -> ReplicationFollower
typealias ReplicationFollowerEntry = Pair<String, ReplicationFollowerFactory>

/**
 * Holds all replication follower implementations.
 */
object ReplicationFollowers {
    val DEMO: ReplicationFollowerEntry = "demo" to ::createDemoReplicationFollower
    val ASYNC: ReplicationFollowerEntry = "async" to { _: NodeAddress, _: Storage, _:MetadataServiceBlockingStub ->
        TODO("Task 4: create your replication follower instance")
    }
    val ALL = listOf(DEMO, ASYNC).toMap()
}

fun createDemoReplicationFollower(
    selfAddress: NodeAddress,
    storage: Storage,
    metadataStub: MetadataServiceBlockingStub
): DemoReplicationFollower = DemoReplicationFollower(storage)

