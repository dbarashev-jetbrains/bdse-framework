package kvas.node.raft

import kvas.node.PostgresConfig
import kvas.proto.KvasReplicationProto
import kvas.util.ObservableProperty

class DatabaseLogStorage(private val config: PostgresConfig) : LogStorage {
    override val lastCommittedEntryNum: ObservableProperty<KvasReplicationProto.LogEntryNumber>
        get() = TODO("Not yet implemented")

    override fun add(entry: KvasReplicationProto.LogEntry) {
        TODO("Not yet implemented")
    }

    override fun createIterator(): LogIterator {
        TODO("Not yet implemented")
    }

    override fun lastOrNull(): KvasReplicationProto.LogEntry? {
        TODO("Not yet implemented")
    }
}