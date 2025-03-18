package kvas.node.raft

import kvas.node.storage.ClusterOutageState
import kvas.node.storage.Storage
import kvas.proto.DataServiceGrpcKt

/**
 * Interface of the Raft leader extension. A node that currently plays the Raft leader role is expected to replicate
 * the incoming updates to the followers by running the Raft log replication protocol.
 */
interface RaftReplicationLeader {
    /**
     * Returns a DataService implementation that internally maintains leader's part of the Raft replication protocol.
     * In particular, PutValue method is supposed to block until a new log entry is replicated or fails to replicate
     * according to Raft AppendLog protocol.
     */
    fun getDataService(): DataServiceGrpcKt.DataServiceCoroutineImplBase

    /**
     * This method will be called when new nodes join a Raft cluster. The implementation may do whatever is
     * required to add the new node as a new follower.
     */
    fun onMetadataChange()
}

typealias ReplicationLeaderFactory = (ClusterState, NodeState, Storage, LogStorage, ClusterOutageState) -> RaftReplicationLeader
typealias ReplicationLeaderProvider = Pair<String, ReplicationLeaderFactory>

object RaftLeaders {
    val DEMO: ReplicationLeaderProvider = "demo" to ::DemoReplicationLeader
    val REAL: ReplicationLeaderProvider = "real" to { _: ClusterState, _: NodeState, _: Storage, _: LogStorage, _:ClusterOutageState ->
        TODO("Task 7: Implement your own RAFT replication leader")
    }
    val ALL = listOf(DEMO, REAL).toMap()
}

