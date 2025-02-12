package kvas.node.raft

import kvas.proto.KvasReplicationProto.LogEntry
import kvas.proto.KvasReplicationProto.LogEntryNumber
import kvas.util.ObservableProperty
import kvas.util.compareTo
import kvas.util.toLogString

interface LogStorage {
    val lastCommittedEntryNum: ObservableProperty<LogEntryNumber>
    fun add(entry: LogEntry)
    fun createView(): LogStorageView
    fun lastOrNull(): LogEntry?
}

interface LogStorageView {
    val lastCommittedEntry: LogEntryNumber

    fun get(): LogEntry?
    fun forward(): Boolean
    fun positionAt(entry: LogEntryNumber)
}

class InMemoryLogStorage : LogStorage {
    private val log = org.slf4j.LoggerFactory.getLogger("Raft.LogStorage")
    private val entries = mutableListOf<LogEntry>()

    override val lastCommittedEntryNum = ObservableProperty<LogEntryNumber>(LogEntryNumber.getDefaultInstance()).also {
        it.subscribe { oldValue, newValue ->
            if (newValue.compareTo(oldValue) < 0) {
                log.error("New value of last committed=$newValue is less than the old value=$oldValue")
                return@subscribe false
            } else return@subscribe true

        }
    }

    override fun lastOrNull(): LogEntry? = synchronized(entries) {
        entries.lastOrNull()
    }

    override fun add(entry: LogEntry) {
        synchronized(entries) {
            entries.add(entry)
        }
        log.debug("Added entry ${entry.entryNumber.toLogString()}. All entries: ${this.toDebugString()}")
    }

    override fun createView(): LogStorageView = RaftLogView(entries)

    fun toDebugString(): String =
        entries.map { it.entryNumber.toLogString() }.joinToString(separator = ", ")

}

/**
 * Represents a view on a sequence of log entries in a Raft system. The view facilitates navigation through the log
 * entries and provides access to specific entries based on position or entry number.
 *
 * @param entries The list of log entries available in this view.
 * @param pos The current position in the list of entries.
 */
internal class RaftLogView(private val entries: List<LogEntry>, private var pos: Int = entries.size) : LogStorageView {
    override val lastCommittedEntry: LogEntryNumber = synchronized(entries) {
        entries.lastOrNull()?.entryNumber ?: LogEntryNumber.getDefaultInstance()
    }

    override fun get(): LogEntry? = synchronized(entries) {
        if (pos >= 0 && entries.size > pos) entries[pos] else null
    }

    override fun forward(): Boolean {
        synchronized(entries) {
            if (pos < entries.size) pos++
            return pos < entries.size
        }
    }

    override fun positionAt(entry: LogEntryNumber) {
        synchronized(entries) {
            if (entry == LogEntryNumber.getDefaultInstance()) {
                pos = -1
                return
            }
            val idx = entries.indexOfLast {
                it.entryNumber == entry
            }
            if (idx != -1) {
                pos = idx
            }
        }
    }
}

