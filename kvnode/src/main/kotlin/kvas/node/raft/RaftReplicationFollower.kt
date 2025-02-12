package kvas.node.raft

import kotlinx.coroutines.runBlocking
import kvas.node.storage.Storage
import kvas.proto.KvasRaftProto
import kvas.proto.KvasRaftProto.RaftAppendLogResponse
import kvas.proto.KvasReplicationProto.LogEntryNumber
import kvas.proto.RaftReplicationServiceGrpcKt
import kvas.proto.raftAppendLogResponse
import kvas.util.compareTo
import kvas.util.toLogString
import org.slf4j.LoggerFactory

object RaftReplicationFollowers {
    val DEMO = "demo" to ::DemoReplicationFollower
    val REAL = "real" to { _: ClusterState, _: NodeState, _: Storage ->
        TODO("Task X: Implement your own RAFT replication follower")
    }
    val ALL = listOf(DEMO, REAL).toMap()
}

class DemoReplicationFollower(
    private val clusterState: ClusterState,
    private val nodeState: NodeState,
    private val storage: Storage
) : RaftReplicationServiceGrpcKt.RaftReplicationServiceCoroutineImplBase() {
    val log = LoggerFactory.getLogger("Raft.Follower")

    override suspend fun appendLog(request: KvasRaftProto.RaftAppendLogRequest): RaftAppendLogResponse {
        return synchronized(nodeState) {
            _appendLog(request)
        }
    }

    fun _appendLog(request: KvasRaftProto.RaftAppendLogRequest): RaftAppendLogResponse {
//        if (request.senderAddress == nodeState.address.toString()) {
//            // log.debug(".. oh, that's me pinging myself, okay")
//            return raftAppendLogResponse {
//                this.status = RaftAppendLogResponse.Status.OK
//                this.lastCommittedEntry = nodeState.logStorage.lastCommittedEntryNum.value
//                this.termNumber = nodeState.currentTerm
//            }
//        }
//
        // Are we sure that the request comes from the most actual leader?

        // Otherwise the request looks legitimate and we start adding the entry to the local log and updating the
        // cluster information.
        if (request.senderAddress != clusterState.leaderAddress.toString()) {
            log.debug("AppendLog from {}", request.senderAddress)
            log.debug(
                "Its term={} my term={}. My last log entry={}",
                request.termNumber,
                nodeState.currentTerm,
                nodeState.logStorage.lastOrNull()?.entryNumber
            )
            log.debug(request.toString())
        }

        clusterState.updateLeader(request)
        nodeState.currentTerm = request.termNumber
        // We consider this request as legitimate, so let's update the leader, term and role.
        if (request.senderAddress != nodeState.address.toString()) {
            nodeState.raftRole.value = RaftRole.FOLLOWER
        }

        if (request.hasEntry()) {
            // If this follower code runs on the same node with the leader then the entry is already in the log storage.
            // Otherwise we need to append it to the log.
            val lastLogEntry = nodeState.logStorage.lastOrNull()?.entryNumber
            val requestLogEntryNum = request.entry.entryNumber
            if (lastLogEntry != null) {
                if (requestLogEntryNum.compareTo(lastLogEntry) < 0) {
                    if (requestLogEntryNum.compareTo(nodeState.logStorage.lastCommittedEntryNum.value) < 0) {
                        println("Last log entry is ${lastLogEntry.toLogString()} request is ${requestLogEntryNum.toLogString()}")
                        return raftAppendLogResponse {
                            this.status = RaftAppendLogResponse.Status.REJECT
                            this.lastCommittedEntry = nodeState.logStorage.lastCommittedEntryNum.value
                            this.termNumber = nodeState.currentTerm
                        }
                    }
                }
                if (requestLogEntryNum.compareTo(lastLogEntry) == 0) {
                    return raftAppendLogResponse {
                        this.status = RaftAppendLogResponse.Status.OK
                        this.lastCommittedEntry = nodeState.logStorage.lastCommittedEntryNum.value
                        this.termNumber = nodeState.currentTerm
                    }
                }
                if (requestLogEntryNum.compareTo(lastLogEntry) > 0) {
                    if (requestLogEntryNum.ordinalNumber != lastLogEntry.ordinalNumber + 1) {
                        return raftAppendLogResponse {
                            this.status = RaftAppendLogResponse.Status.LOG_MISMATCH
                            this.lastCommittedEntry = nodeState.logStorage.lastCommittedEntryNum.value
                            this.termNumber = nodeState.currentTerm
                        }
                    } else {
                        println("FOLLOWER: !!! added ${request.entry.entryNumber.toLogString()} (last log=${lastLogEntry.toLogString()})")
                        nodeState.logStorage.add(request.entry)
                    }
                }
            } else {
                println("FOLLOWER: !!! added ${request.entry.entryNumber.toLogString()}")
                nodeState.logStorage.add(request.entry)
            }
        }

        val lastCommittedEntryNum = nodeState.logStorage.lastCommittedEntryNum.value
        if (lastCommittedEntryNum.compareTo(request.lastCommittedEntry) < 0) {
            log.debug("Last committed entry on this node: {}", lastCommittedEntryNum.toLogString())
            log.info(
                "We will commit local log entries: ({} .. {}]",
                lastCommittedEntryNum.toLogString(),
                request.lastCommittedEntry.toLogString()
            )
            // If the leader's last committed entry is > than ours, we start committing our log until we
            // reach the last committed on the server or the end of the log.
            val lastCommitted = commitRange(lastCommittedEntryNum, request.lastCommittedEntry)
            nodeState.logStorage.lastCommittedEntryNum.value = lastCommitted
            if (lastCommitted != request.lastCommittedEntry) {
                // It is possible that the log on this node is not yet complete, because the replication is lagging.
                // A missing entry is expected to replicate soon.
                log.warn("The log on this node is not yet complete: ${nodeState.logStorage.toDebugString()}")
                return raftAppendLogResponse {
                    this.status = RaftAppendLogResponse.Status.LOG_MISMATCH
                    lastCommittedEntry = nodeState.logStorage.lastCommittedEntryNum.value
                    termNumber = nodeState.currentTerm
                }
            }
        }

        return raftAppendLogResponse {
            this.status = RaftAppendLogResponse.Status.OK
            lastCommittedEntry = nodeState.logStorage.lastCommittedEntryNum.value
            termNumber = nodeState.currentTerm
        }
    }


    private fun commitRange(firstEntry: LogEntryNumber, lastEntry: LogEntryNumber): LogEntryNumber {
        val log = LoggerFactory.getLogger("Raft.Follower.CommitLog")
        var lastCommitted = firstEntry
        val commitView = nodeState.logStorage.createView()

        commitView.positionAt(firstEntry)
        commitView.forward()
        while (true) {
            val isBreak = commitView.get()?.let {
                if (it.entryNumber.compareTo(lastEntry) > 0) {
                    log.debug(
                        "Entry in the local log={} is > last committed on the leader={}, breaking",
                        it.entryNumber.toLogString(),
                        lastEntry.toLogString()
                    )
                    true
                } else {
                    log.debug("committing entry {}", it)
                    runBlocking {
                        it.dataRow.valuesMap.forEach({ column, value ->
                            storage.put(it.dataRow.key, column, value)
                        })
                    }
                    lastCommitted = it.entryNumber
                    commitView.forward()
                    false
                }
            } ?: true
            if (isBreak) {
                break
            }
        }
        log.info("Committed the log until entry {}", lastCommitted.toLogString())
        return lastCommitted
    }

}


