package kvas.client

/**
 * Represents the Percolator Protocol, used for reading and updating data in KVAS storage with ACID properties.
 */
interface PercolatorProtocol {
    /**
     * Returns the value of the requested cell following Percolator's Snapshot Isolation guarantee.
     */
    fun get(getCommand: GetCommand): String?

    /**
     * Commits the given list of [PutCommand]s to the underlying KVAS storage, following Percolator protocol.
     */
    fun commit(putCommands: List<PutCommand>)
}

typealias TimestampGenerator = () -> Long
typealias PercolatorProtocolFactory = (VersionedKvasClient, TimestampGenerator) -> PercolatorProtocol
typealias PercolatorProvider = Pair<String, PercolatorProtocolFactory>

/**
 * All available Percolator protocol implementations.
 *
 * @property DEMO A demo implementation that does not provide ACID guarantees.
 * @property REAL The real implementation that provides ACID guarantees.
 * @property ALL A map of all available Percolator protocol implementations.
 */
object PercolatorProtocols {
    val DEMO: PercolatorProvider = "demo" to ::DemoPercolator
    val REAL: PercolatorProvider = "real" to { _, _ -> TODO("Task 8: Implement the real Percolator protocol")}
    val ALL = listOf(DEMO, REAL).toMap()
}

/**
 * This is a demo implementation that doesn't really follow Percolator protocol. It returns the latest available
 * version on GET request and just writes all the data on commit.
 */
class DemoPercolator(private val versionedKvasClient: VersionedKvasClient, private val timestampGenerator: TimestampGenerator) : PercolatorProtocol {
    private val startTimestamp = timestampGenerator()

    init {
        println("Started Percolator at $startTimestamp")
    }
    override fun get(getCommand: GetCommand): String? =
        versionedKvasClient.get(getCommand.key, getCommand.columnName)

    override fun commit(putCommands: List<PutCommand>) {
        val commitTimestamp = timestampGenerator()
        putCommands.forEach { versionedKvasClient.put(it.key, it.columnName, it.value) }
        println("Committed Percolator at $commitTimestamp")
    }
}

