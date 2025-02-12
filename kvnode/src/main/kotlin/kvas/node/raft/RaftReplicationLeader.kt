package kvas.node.raft

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

object RaftReplicationLeaders {
    val DEMO = "demo" to ::DemoReplicationLeader
    val REAL = "real" to { _: ClusterState, _: NodeState, _: Storage, _: LogStorage ->
        TODO("Task X: Implement your own RAFT replication leader")
    }
    val ALL = listOf(DEMO, REAL).toMap()
}

