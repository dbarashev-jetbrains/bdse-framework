package kvas.node.raft

import kotlinx.coroutines.runBlocking
import kvas.node.storage.Storage
import kvas.node.storage.globalPostgresConfig
import kvas.proto.KvasReplicationProto.LogEntry
import kvas.proto.KvasReplicationProto.LogEntryNumber
import kvas.util.ObservableProperty
import kvas.util.compareTo
import kvas.util.toLogString
import org.slf4j.LoggerFactory

/**
 * Interface representing a storage mechanism for log entries in a Raft node,
 */
interface LogStorage {
    /**
     * The number of the last entry that is committed on this node.
     * This property is observable, and observers may do something when the last committed entry updates.
     *
     * The value defaults to the default instance of LogEntryNumber if no entries are committed.
     */
    val lastCommittedEntryNum: ObservableProperty<LogEntryNumber>

    /**
     * Appends a log entry to the log.
     */
    fun add(entry: LogEntry)

    /**
     * Creates an iterable view over the log.
     */
    fun createIterator(): LogIterator

    /**
     * Returns the last entry in the log or null if the log is empty.
     */
    fun lastOrNull(): LogEntry?
}

/**
 * Represents an iterator over a sequence of log entries. The iterator supports navigation through the log
 * entries and provides access to the entry at the specified position.
 */
interface LogIterator {
    val lastCommittedEntry: LogEntryNumber

    fun get(): LogEntry?
    fun forward(): Boolean
    fun positionAt(entry: LogEntryNumber)
}

/**
 * All available implementations of the log storage.
 */
object LogStorages {
    val IN_MEMORY = "memory" to ::InMemoryLogStorage
    val DBMS = "dbms" to {
        DatabaseLogStorage(
            globalPostgresConfig
                ?: error("Please specify DBMS connection options using --storage dbms command line flag")
        )
    }

    val ALL = listOf(IN_MEMORY, DBMS).toMap()
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

    override fun createIterator(): LogIterator = InMemoryLogIterator(entries)

    override fun toString() = toDebugString()

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
internal class InMemoryLogIterator(private val entries: List<LogEntry>, private var pos: Int = entries.size) :
    LogIterator {
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

fun LogStorage.commitRange(storage: Storage, firstEntry: LogEntryNumber, lastEntry: LogEntryNumber): LogEntryNumber {
    val log = LoggerFactory.getLogger("Raft.Follower.CommitLog")
    var lastCommitted = firstEntry
    val commitView = this.createIterator()

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
    lastCommittedEntryNum.value = lastCommitted
    return lastCommitted
}
