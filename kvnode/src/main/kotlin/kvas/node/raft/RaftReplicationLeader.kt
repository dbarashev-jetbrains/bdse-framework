package kvas.node.raft

import kvas.node.storage.ClusterOutageState
import kvas.node.storage.Storage
import kvas.proto.DataServiceGrpcKt

/**
 * Interface of the Raft leader extension. A node that currently plays the Raft leader role is expected to replicate
 * the incoming updates to the followers by running the Raft log replication protocol.
 */
interface RaftReplicationLeader {
    fun getDataService(): DataServiceGrpcKt.DataServiceCoroutineImplBase
    fun onMetadataChange()
}

typealias ReplicationLeaderFactory = (ClusterState, NodeState, Storage, LogStorage, ClusterOutageState) -> RaftReplicationLeader
typealias ReplicationLeaderProvider = Pair<String, ReplicationLeaderFactory>

object RaftLeaders {
    val DEMO: ReplicationLeaderProvider = "demo" to ::DemoReplicationLeader
    val REAL: ReplicationLeaderProvider = "real" to { _: ClusterState, _: NodeState, _: Storage, _: LogStorage, _:ClusterOutageState ->
        TODO("Task X: Implement your own RAFT replication leader")
    }
    val ALL = listOf(DEMO, REAL).toMap()
}

