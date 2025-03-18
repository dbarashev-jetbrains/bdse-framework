package kvas.node.raft

import kvas.proto.KvasReplicationProto
import kvas.util.ObservableProperty
import javax.sql.DataSource

class DatabaseLogStorage(private val dataSource: DataSource) : LogStorage {
    override val lastCommittedEntryNum: ObservableProperty<KvasReplicationProto.LogEntryNumber>
        get() = TODO("Task 6: Implement a persistent log storage")

    override fun add(entry: KvasReplicationProto.LogEntry) {
        TODO("Task 6: Implement a persistent log storage")
    }

    override fun createIterator(): LogIterator {
        TODO("Task 6: Implement a persistent log storage")
    }

    override fun lastOrNull(): KvasReplicationProto.LogEntry? {
        TODO("Task 6: Implement a persistent log storage")
    }
}