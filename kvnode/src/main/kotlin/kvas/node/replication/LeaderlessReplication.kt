package kvas.node.replication

import kvas.node.OnMetadataChange
import kvas.node.storage.Storage
import kvas.proto.DataServiceGrpcKt
import kvas.proto.KvasMetadataProto
import kvas.proto.KvasProto
import kvas.proto.KvasProto.GetValueRequest
import kvas.proto.KvasProto.GetValueResponse
import kvas.proto.KvasProto.PutValueResponse
import kvas.proto.clusterMetadata
import org.slf4j.LoggerFactory

interface LeaderlessReplicationNode {
    val coordinatorStorage: Storage

    val metadataListener: OnMetadataChange
    fun getValue(req: GetValueRequest): GetValueResponse
    fun putValue(req: KvasProto.PutValueRequest): PutValueResponse
    fun onRegister(registerNodeResponse: KvasProto.RegisterNodeResponse)
}

typealias LeaderlessReplicationNodeFactory = (Storage) -> LeaderlessReplicationNode
typealias LeaderlessReplicationEntry = Pair<String, LeaderlessReplicationNodeFactory>

object LeaderlessReplication {
    val DEMO: LeaderlessReplicationEntry = "demo" to ::DemoLeaderlessReplicationNode
    val REAL: LeaderlessReplicationEntry = "real" to { _: Storage -> TODO() }

    val ALL = listOf(DEMO, REAL).toMap()
}

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
class DemoLeaderlessReplicationNode(private val delegateStorage: Storage): LeaderlessReplicationNode {
    private var clusterMetadata = clusterMetadata {  }

    override val coordinatorStorage: Storage
        get() = delegateStorage

    override val metadataListener: OnMetadataChange
        get() = ::onMetadataChange

    override fun getValue(req: GetValueRequest): GetValueResponse {
        TODO("Not yet implemented")
    }

    override fun putValue(req: KvasProto.PutValueRequest): PutValueResponse {
        TODO("Not yet implemented")
    }

    override fun onRegister(registerNodeResponse: KvasProto.RegisterNodeResponse) {
        onMetadataChange(registerNodeResponse.metadata)
    }

    fun onMetadataChange(clusterMetadata: KvasMetadataProto.ClusterMetadata) {
        this.clusterMetadata = clusterMetadata
        println("New cluster metadata: $clusterMetadata")
    }

}

private val LOG_GET = LoggerFactory.getLogger("Node.GetValue.Leaderless")
private val LOG_PUT = LoggerFactory.getLogger("Node.PutValue.Leaderless")