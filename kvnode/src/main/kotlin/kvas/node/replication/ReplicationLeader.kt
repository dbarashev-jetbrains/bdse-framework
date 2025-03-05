package kvas.node.replication

import io.grpc.ManagedChannelBuilder
import kvas.node.OnMetadataChange
import kvas.node.storage.RowScan
import kvas.node.storage.Storage
import kvas.node.storage.StoredRow
import kvas.proto.KvasMetadataProto.ClusterMetadata
import kvas.proto.MetadataServiceGrpc.MetadataServiceBlockingStub
import kvas.proto.ReplicationFollowerGrpc
import kvas.proto.ReplicationFollowerGrpc.ReplicationFollowerBlockingStub
import kvas.proto.appendLogRequest
import kvas.proto.dataRow
import kvas.proto.logEntry
import kvas.util.NodeAddress
import kvas.util.toNodeAddress
import org.slf4j.LoggerFactory

/**
 * Follower address and GRPC stub to send messages.
 */
data class Replica(val address: NodeAddress, var stub: ReplicationFollowerBlockingStub)

/**
 * A replication leader implements:
 * - storage interface that is actually responsible for the replication procedure in
 * its `put` method.
 * - metadata listener that is responsible for maintaining the list of replicas.
 */
interface ReplicationLeader {
    /**
     * Creates and returns a `Storage` instance that is capable of replicating values, usually when handling `put`
     * operations.
     */
    fun createStorage(): Storage

    /**
     * Creates a listener on metadata changes. The listener will be called whenever a node receives a metadata change
     * event from the metadata master.
     */
    fun createMetadataListener(): OnMetadataChange
}

typealias ReplicationLeaderFactory = (NodeAddress, Storage, MetadataServiceBlockingStub) -> ReplicationLeader
typealias ReplicationLeaderEntry = Pair<String, ReplicationLeaderFactory>

/**
 * Holds all replication leader implementations, as a mapping of the implementation name to the function that creates
 * a ReplicationLeader instance.
 */
object ReplicationLeaders {
    val VOID: ReplicationLeaderEntry = "void" to ::createVoidReplicationLeader
    val DEMO: ReplicationLeaderEntry = "demo" to ::DemoReplicationLeader
    val ASYNC: ReplicationLeaderEntry = "async" to { _: NodeAddress, _: Storage, _:MetadataServiceBlockingStub ->
        TODO("Task 4: implement asynchronous replication")
    }
    val ALL = listOf(VOID, DEMO, ASYNC).toMap()
}

/**
 * This replication leader makes no replication.
 */
class VoidReplicationLeader(
    private val storageDelegate: Storage,
    private val metadataStub: MetadataServiceBlockingStub
) : ReplicationLeader {
    override fun createStorage(): Storage = storageDelegate
    override fun createMetadataListener(): OnMetadataChange = {}
}

fun createVoidReplicationLeader(selfAddress: NodeAddress, storage: Storage, metadataStub: MetadataServiceBlockingStub) =
    VoidReplicationLeader(storage, metadataStub)

/**
 * Demo replication storage writes the values locally and then sends out AppendLog messages to all replicas.
 * There are no safety guarantees. This protocol works only if all nodes and network connections do not fail, and
 * there are no concurrent put requests.
 */
class DemoReplicationLeaderStorage(
    private val storageDelegate: Storage,
    private val selfAddress: NodeAddress,
    private val replicas: List<Replica>
) : Storage {
    override fun put(rowKey: String, columnName: String, value: String) {
        storageDelegate.put(rowKey, columnName, value)
        replicas.forEach {
            it.stub.appendLog(appendLogRequest {
                senderAddress = selfAddress.toString()
                entries.add(logEntry {
                    dataRow = dataRow {
                        key = rowKey
                        values[columnName] = value
                    }
                })
            })
        }
    }

    override fun get(rowKey: String, columnName: String): String? = storageDelegate.get(rowKey, columnName)
    override fun getRow(rowKey: String): StoredRow? = storageDelegate.getRow(rowKey)

    override fun scan(conditions: Map<String, String>): RowScan = storageDelegate.scan(conditions)

    override val supportedFeatures: Map<String, String> get() = storageDelegate.supportedFeatures
}

/**
 * This is a very simple implementation of the replication leader. It maintains the list of replicas, as they come from the
 * metadata server, and uses a naive replication storage that sends out updates immediately as they come without any
 * safety guarantees.
 */
class DemoReplicationLeader(
    private val selfAddress: NodeAddress,
    private val storageDelegate: Storage,
    private val metadataStub: MetadataServiceBlockingStub
) : ReplicationLeader {
    private val replicaList = mutableListOf<Replica>()
    private var clusterMetadata: ClusterMetadata = ClusterMetadata.getDefaultInstance()

    override fun createStorage(): Storage {
        return DemoReplicationLeaderStorage(storageDelegate, selfAddress, replicaList)
    }

    override fun createMetadataListener(): OnMetadataChange = { clusterMetadata ->
        this.clusterMetadata = clusterMetadata
        this.replicaList.clear()
        clusterMetadata.shardsList.find { it.leader.nodeAddress == selfAddress.toString() }?.let { shard ->
            shard.followersList.map {
                val replicaAddress = it.nodeAddress.toNodeAddress()
                val channel =
                    ManagedChannelBuilder.forAddress(replicaAddress.host, replicaAddress.port).usePlaintext().build()
                Replica(replicaAddress, ReplicationFollowerGrpc.newBlockingStub(channel))
            }
        }?.let { this.replicaList.addAll(it) }
        LOG.debug("New replica list: {}", replicaList)
    }

}

private val LOG = LoggerFactory.getLogger("Replication.Leader")