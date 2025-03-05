package kvas.node.replication

import com.google.protobuf.StringValue
import io.grpc.ManagedChannelBuilder
import kvas.node.OnMetadataChange
import kvas.node.storage.Storage
import kvas.proto.*
import kvas.proto.DataServiceGrpc.DataServiceBlockingStub
import kvas.proto.KvasProto.GetValueRequest
import kvas.proto.KvasProto.GetValueResponse
import kvas.proto.KvasProto.PutValueResponse
import kvas.util.KvasPool
import kvas.util.NodeAddress
import kvas.util.toNodeAddress
import org.slf4j.LoggerFactory

/**
 * Leaderless replication node. Its methods are equivalent to the methods of DataService, and KVAS framework will
 * wrap them into DataService instances.
 */
interface LeaderlessReplicationNode {
    /**
     * This listener will be called on metadata changes.
     */
    val metadataListener: OnMetadataChange

    /**
     * This method may be called by both external clients and "internally" by ambassador nodes.
     * Use GetValueRequest::isAmbassador field to distinguish external and internal requests.
     */
    fun getValue(req: GetValueRequest): GetValueResponse

    /**
     * This method may be called by both external clients and "internally" by ambassador nodes.
     * Use GetValueRequest::isAmbassador field to distinguish external and internal requests.
     */
    fun putValue(req: KvasProto.PutValueRequest): PutValueResponse
}

typealias LeaderlessReplicationNodeFactory = (Storage, NodeAddress) -> LeaderlessReplicationNode
typealias LeaderlessReplicationEntry = Pair<String, LeaderlessReplicationNodeFactory>

/**
 * Holds all available implementations of the leaderless replication nodes.
 */
object LeaderlessReplication {
    val DEMO: LeaderlessReplicationEntry = "demo" to ::DemoLeaderlessReplicationNode
    val REAL: LeaderlessReplicationEntry = "real" to { _: Storage, _:NodeAddress ->
        TODO("Task 5: implement leaderless replication")
    }

    val ALL = listOf(DEMO, REAL).toMap()
}

/**
 * A data service implementation that just delegates the method calls to the LeaderlessReplicationNode instance.
 * You don't need to change this code.
 */
class LeaderlessReplicationDataServiceImpl(private val leaderlessNode: LeaderlessReplicationNode) : DataServiceGrpcKt.DataServiceCoroutineImplBase() {
    override suspend fun getValue(request: GetValueRequest): GetValueResponse  = try {
        leaderlessNode.getValue(request)
    } catch (ex: Exception) {
        LOG_GET.error("Failure when processing getValue({}, {})", request.rowKey, request.columnName, ex)
        LOG_GET.debug("Request: {}", request)
        throw ex
    }

    override suspend fun putValue(request: KvasProto.PutValueRequest): PutValueResponse = try {
        leaderlessNode.putValue(request)
    } catch (ex: Exception) {
        LOG_PUT.error("Failure when processing putValue({}, {})", request.rowKey, request.columnName, ex)
        LOG_PUT.debug("Request: {}", request)
        throw ex
    }
}

internal typealias LeaderlessReplica = NodeAddress

/**
 * Demo implementation of the leaderless replication. It just forwards all external PUT requests to the other replicas.
 */
internal class DemoLeaderlessReplicationNode(private val storage: Storage, private val selfAddress: NodeAddress): LeaderlessReplicationNode {
    private var clusterMetadata = clusterMetadata {  }
    private val replicaList = mutableListOf<LeaderlessReplica>()
    private val grpcPool = KvasPool<DataServiceBlockingStub>(selfAddress) {
        ManagedChannelBuilder.forAddress(it.host, it.port).usePlaintext().build().let { channel ->
            DataServiceGrpc.newBlockingStub(channel)
        }
    }

    override val metadataListener: OnMetadataChange
        get() = ::onMetadataChange

    override fun getValue(req: GetValueRequest): GetValueResponse {
        storage.getRow(req.rowKey)?.let { row ->
            row.valuesMap.get(req.columnName)?.let { value ->
                return getValueResponse {
                    this.value = StringValue.of(value)
                    this.version = row.version
                    this.code = GetValueResponse.StatusCode.OK
                }
            }
        }
        return getValueResponse {
            this.code = GetValueResponse.StatusCode.OK
        }
    }

    override fun putValue(req: KvasProto.PutValueRequest): PutValueResponse {
        storage.put(req.rowKey, req.columnName, req.value)
        if (!req.isAmbassador) {
            replicaList.forEach { replica ->
                grpcPool.rpc(replica) {
                    putValue(putValueRequest {
                        this.rowKey = req.rowKey
                        this.columnName = req.columnName
                        this.value = req.value
                        this.isAmbassador = true
                    })
                }
            }
        }
        return putValueResponse {
            this.code = PutValueResponse.StatusCode.OK
            this.version = storage.getRow(req.rowKey)?.version ?: -1L
        }
    }

    fun onMetadataChange(clusterMetadata: KvasMetadataProto.ClusterMetadata) {
        this.clusterMetadata = clusterMetadata
        replicaList.clear()
        replicaList.addAll(clusterMetadata.shardsList.find { it.shardToken == 0 }?.let { shard ->
            shard.followersList.map { it.nodeAddress.toNodeAddress() } + listOf(shard.leader.nodeAddress.toNodeAddress())
        }?.filter { it != selfAddress } ?: emptyList())
    }
}

private val LOG_GET = LoggerFactory.getLogger("Node.GetValue.Leaderless")
private val LOG_PUT = LoggerFactory.getLogger("Node.PutValue.Leaderless")