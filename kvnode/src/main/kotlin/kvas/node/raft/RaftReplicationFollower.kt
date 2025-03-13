package kvas.node.raft

import kotlinx.coroutines.runBlocking
import kvas.node.storage.Storage
import kvas.proto.KvasRaftProto
import kvas.proto.KvasRaftProto.RaftAppendLogResponse
import kvas.proto.KvasReplicationProto.LogEntryNumber
import kvas.proto.raftAppendLogResponse
import kvas.util.compareTo
import kvas.util.toLogString
import org.slf4j.LoggerFactory

interface AppendLogProtocol {
    fun appendLog(request: KvasRaftProto.RaftAppendLogRequest): RaftAppendLogResponse
}

typealias AppendLogProtocolFactory = (ClusterState, NodeState, Storage) -> AppendLogProtocol
typealias AppendLogProtocolProvider = Pair<String, AppendLogProtocolFactory>

object RaftFollowers {
    val DEMO: AppendLogProtocolProvider = "demo" to ::DemoReplicationFollower
    val REAL: AppendLogProtocolProvider = "real" to { _: ClusterState, _: NodeState, _: Storage ->
        TODO("Task X: Implement your own RAFT replication follower")
    }
    val ALL = listOf(DEMO, REAL).toMap()
}

/**
 * This is a DEMO implementation of the RAFT AppendLog protocol on the follower side.
 * It works in the basic success case when the cluster nodes do not fail and network partitions do not happen.
 */
class DemoReplicationFollower(
    private val clusterState: ClusterState,
    private val nodeState: NodeState,
    private val storage: Storage
) : AppendLogProtocol {
    val log = LoggerFactory.getLogger("Raft.Follower")

    override fun appendLog(request: KvasRaftProto.RaftAppendLogRequest): RaftAppendLogResponse = try {
        synchronized(nodeState) {
            _appendLog(request)
        }
    } catch (ex: Throwable) {
        log.error("Failure when processing AppendLog", ex)
        throw ex
    }

    fun _appendLog(request: KvasRaftProto.RaftAppendLogRequest): RaftAppendLogResponse {
        if (request.senderAddress != clusterState.leaderAddress.toString()) {
            // It is a surprise, however, it is possible that this node is unaware of a new leader.
            // If the request passes some sanity checks, it is okay to recognize the sender as a new leader.
            // However, this is a demo, so no sanity checks are implemented.
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

        if (request.senderAddress != nodeState.address.toString()) {
            nodeState.raftRole.value = RaftRole.FOLLOWER
        }

        // Some requests come without any log entry, they are just heartbeats. If there is an entry, let's
        // see if we need to append it to the log.
        if (request.hasEntry()) {
            // The entry that is being replicated may or may not be in the local log storage.
            // The usual case when it is already in the storage is when this code is running on the leader:
            // we have already put the entry to the log before we started replication, and now we're "replicating"
            // it to ourselves.
            // However, it may as well be in the log because of other reasons, e.g. when a new leader completes
            // the replication that was terminated with the death of the old one.

            val requestLogEntryNum = request.entry.entryNumber
            val lastLogEntry = nodeState.logStorage.lastOrNull()?.entryNumber

            if (lastLogEntry == null) {
                // This is the success case: our replication log is empty!
                log.debug("Entry {} is appended to the local log", requestLogEntryNum)
                nodeState.logStorage.add(request.entry)
            } else  {
                val compareResult = requestLogEntryNum.compareTo(lastLogEntry)
                when {
                    compareResult > 0 -> {
                        // This is probably the success case: the replicated entry immediately follows the last entry
                        // in the local replication log.
                        if (requestLogEntryNum.ordinalNumber == lastLogEntry.ordinalNumber + 1) {
                            log.debug("Entry {} is appended to the local log", requestLogEntryNum)
                            nodeState.logStorage.add(request.entry)
                        } else {
                            // But it is also possible thst this node is lagging behind the leader.
                            return raftAppendLogResponse {
                                this.status = RaftAppendLogResponse.Status.LOG_MISMATCH
                                this.lastCommittedEntry = nodeState.logStorage.lastCommittedEntryNum.value
                                this.termNumber = nodeState.currentTerm
                            }
                        }
                    }
                    compareResult == 0 -> {
                        // We have this entry in our local replication log.  We do not return, because it is possible,
                        // that the last committed entry is different.
                        log.debug("Entry {} is already in the local log", requestLogEntryNum)
                    }
                    compareResult < 0 -> {
                        // Well, we have something older in the local replication log.
                        if (requestLogEntryNum.compareTo(nodeState.logStorage.lastCommittedEntryNum.value) < 0) {
                            // If the older entry is committed, then the sender is likely to be slightly out of date
                            // (perhaps the sender is a former leader who is unaware of the presence of a new one).
                            log.warn("The last committed entry={} > the request entry={}", nodeState.logStorage.lastCommittedEntryNum.value, requestLogEntryNum)
                            return raftAppendLogResponse {
                                this.status = RaftAppendLogResponse.Status.REJECT
                                this.lastCommittedEntry = nodeState.logStorage.lastCommittedEntryNum.value
                                this.termNumber = nodeState.currentTerm
                            }
                        } else {
                            // It is possible that the local replication log contains some entries that never
                            // committed (because their leader didn't manage to gather a quorum).
                            // But as this is a demo, we don't care too much.
                            return raftAppendLogResponse {
                                this.status = RaftAppendLogResponse.Status.LOG_MISMATCH
                                this.lastCommittedEntry = nodeState.logStorage.lastCommittedEntryNum.value
                                this.termNumber = nodeState.currentTerm
                            }
                        }
                    }
                }
            }
        }

        // We get here if the request contains no replicated entry or if the replicated entry was successfully
        // added to the local log. We need to check if we need to commit some of the log entries to match the last
        // committed entry in the request.
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
            val lastCommitted = nodeState.logStorage.commitRange(storage, lastCommittedEntryNum, request.lastCommittedEntry)
            if (lastCommitted != request.lastCommittedEntry) {
                // It is possible that the log on this node is not yet complete, because the replication is lagging.
                // A missing entry is expected to replicate soon.
                log.warn("The log on this node is not yet complete: ${nodeState.logStorage.toString()}")
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
}


