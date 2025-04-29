package kvas.client

/**
 * Base class for all supported shell types. Provides a main loop that reads user input and processes it.
 * The following commands are supported:
 *
 * - `/exit`: Exits the shell.
 * - `/offline <node-id>`: Sets the specified node to offline mode.
 * - `/online <node-id>`: Sets the specified node to online mode.
 * - `/commit`: Commits all data operations that have been performed since the last commit.
 *
 * Input that is not prefixed with slash is considered to be a data command. Data command processing is delegated to
 * processDataOperation() method that shall be implemented by the concrete subclass.
 *
 *
 * @property kvasClient The [KvasClient] instance used to communicate with the KVAS cluster.
 */
abstract class Shell(protected val kvasClient: KvasClient) {
    fun mainLoop() {
        while (true) {
            val input = readlnOrNull() ?: break
            if (input.isBlank()) {
                break
            }
            if (input.startsWith("/")) {
                processCommand(input)
            } else {
                processDataOperation(input)
            }
        }
    }

    protected open fun processCommand(input: String) {
        val words = input.split(" ")
        when (words[0]) {
            "/exit" -> System.exit(0)
            "/offline" -> {
                kvasClient.setAvailable(words.drop(1), false)
            }
            "/online" -> {
                kvasClient.setAvailable(words.drop(1), true)
            }
        }
    }

    protected abstract fun processDataOperation(input: String)
}

/**
 * This is a simple shell that processes data commands in the form of `key[.column]=value` (PUT command)
 * or `key[.column]` (GET command).
 *
 * The column name part is optional. If the column name part is omitted, the default column is used.
 */
class SimpleShell(kvasClient: KvasClient) : Shell(kvasClient) {
    override fun processDataOperation(input: String) {
        val keyValue = input.split("=", limit = 2)
        if (keyValue.size == 2) {
            val splitKey = keyValue[0].split(".", limit = 2)
            if (splitKey.size == 2) {
                kvasClient.put(key = splitKey[0], columnName = splitKey[1], value = keyValue[1])
            } else {
                kvasClient.put(key = keyValue[0], columnName = "", value = keyValue[1])
            }
            println("... done")
        } else {
            val splitKey = keyValue[0].split(".", limit = 2)
            val value = if (splitKey.size == 2) {
                kvasClient.doGet(splitKey[0], splitKey[1])
            } else {
                kvasClient.doGet(keyValue[0])

            }
            println("$keyValue=$value")
        }
    }
}

sealed class DataCommand(val key: String, val columnName: String, val version: Int?)
class GetCommand(key: String, columnName: String, version: Int?) : DataCommand(key, columnName, version) {
    override fun toString(): String {
        return "$key${columnName.takeIf { it.isNotEmpty() }?.let { ".$it" } ?: ""}${version?.let { "@$it" } ?: "" }"
    }
}
class PutCommand(key: String, columnName: String, version: Int?, val value: String) : DataCommand(key, columnName, version)

fun parseDataCommand(input: String): DataCommand {
    var rowKey = ""
    var columnName = ""
    var value = ""
    var version: Int? = null

    val keyValue = input.split("=", limit = 2)
    return if (keyValue.size == 2) {
        value = keyValue[1]
        val splitKey = keyValue[0].split(".", limit = 2)
        if (splitKey.size == 2) {
            columnName = splitKey[1]
        }
        val splitRowKey = splitKey[0].split("@", limit = 2)
        if (splitRowKey.size == 2) {
            version = splitRowKey[1].toIntOrNull()
        }
        rowKey = splitRowKey[0]
        PutCommand(rowKey, columnName, version, value)
    } else {
        val splitKey = keyValue[0].split(".", limit = 2)
        if (splitKey.size == 2) {
            columnName = splitKey[1]
        }
        val splitRowKey = splitKey[0].split("@", limit = 2)
        if (splitRowKey.size == 2) {
            version = splitRowKey[1].toIntOrNull()
        }
        rowKey = splitRowKey[0]
        GetCommand(rowKey, columnName, version)
    }
}

/**
 * This is an extension of the simple shell, that can process data commands with the version numbers:
 *
 * PUT command: key[@version][.column]=value
 * GET command: key[@version][.column]
 *
 * The version number is optional. If the version number is omitted, the latest version is used.
 * The column name part is optional. If the column name part is omitted, the default column is used.
 *
 * @property versionedKvasClient The [VersionedKvasClient] instance used to communicate with the KVAS cluster.
 */
class VersionedShell(private val versionedKvasClient: VersionedKvasClient): Shell(versionedKvasClient.kvasClient) {
    override fun processDataOperation(input: String) {
        val dataCommand = parseDataCommand(input)
        when (dataCommand) {
            is GetCommand -> {
                val value = versionedKvasClient.get(dataCommand.key, dataCommand.columnName, dataCommand.version)
                println("GET: ${dataCommand}=$value")
            }
            is PutCommand -> {
                versionedKvasClient.put(dataCommand.key, dataCommand.columnName,dataCommand.value, dataCommand.version)
                println("...done")
            }
        }
    }
}

/**
 * This is an extension of the simple shell, that works according to Percolator protocol.
 * The data commands have the same format as in the simple shell:
 *
 * PUT: `key[.column]=value`
 * GET: `key[.column]`
 *
 * This shell caches all PUT commands until the user types `/commit` command. After that, the cached data commands
 * shell be written to the storage using Percolator transaction protocol.
 *
 * @property versionedKvasClient The [VersionedKvasClient] instance used to communicate with the KVAS cluster.
 */
class PercolatorShell(private val versionedKvasClient: VersionedKvasClient, percolatorImpl: String): Shell(versionedKvasClient.kvasClient) {
    private val cachedDataCommands = mutableListOf<PutCommand>()
    private val percolator = PercolatorProtocols.ALL[percolatorImpl]?.invoke(versionedKvasClient, this::generateTimestamp)
        ?: error("Invalid percolator implementation: $percolatorImpl")

    override fun processDataOperation(input: String) {
        val dataCommand = parseDataCommand(input)
        when (dataCommand) {
            is GetCommand -> processGetCommand(dataCommand)
            is PutCommand -> processPutCommand(dataCommand)
        }
    }

    override fun processCommand(input: String) {
        val words = input.split(" ")
        when (words[0]) {
            "/commit" -> processCommit()
            else -> super.processCommand(input)
        }
    }

    private fun processGetCommand(dataCommand: GetCommand) {
        val value = percolator.get(dataCommand)
        println("GET: ${dataCommand}=$value")
    }

    private fun processPutCommand(dataCommand: PutCommand) {
        cachedDataCommands.add(dataCommand)
        println("...done")
    }

    private fun processCommit() {
        if (cachedDataCommands.isEmpty()) {
            println("Nothing to commit")
            return
        }
        percolator.commit(cachedDataCommands)
        println("Exiting now")
        System.exit(0)
    }

    fun generateTimestamp(): Long {
        val ts = (versionedKvasClient.get("__nextTxnTimestamp", "")?.toLongOrNull() ?: 0L) + 1
        versionedKvasClient.put("__nextTxnTimestamp", "", ts.toString())
        return ts
    }
}

private fun KvasClient.setAvailable(words: List<String>, isAvailable: Boolean) {
    if (words.isEmpty()) {
        println("Invalid number of arguments")
        return
    }
    words.forEach { word ->
        if (word.indexOf("..") < 0) {
            this.sendNodeAvailable(word, isAvailable)
        } else {
            val (src, dst) = word.split("..", limit = 2)
            this.sendLinkAvailable(src, dst, isAvailable)
            this.sendLinkAvailable(dst, src, isAvailable)
        }
    }
}
