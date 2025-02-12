package kvas.node.storage

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kvas.node.PostgresConfig
import java.sql.SQLException
import javax.sql.DataSource

fun createDataSource(config: PostgresConfig): DataSource {
    val hikariConfig = HikariConfig().apply {
        username = config.dbUser
        password = config.dbPassword
        jdbcUrl = "jdbc:postgresql://${config.dbHost}:${config.dbPort}/${config.dbDatabase.ifEmpty { config.dbUser }}"
        addDataSourceProperty("ssl.mode", "disable");
    }
    return HikariDataSource(hikariConfig)
}

/**
 * Storage implementation that stores data in a relational database.
 */
class DatabaseStorage(private val dataSource: DataSource, private val config: PostgresConfig) : Storage {
    private val isReady by lazy {
        this._isReady()
    }

    private fun _isReady(): Boolean {
        try {
            dataSource.connection.use { conn ->
                val statement = conn.prepareStatement("SELECT COUNT(*) FROM pg_stat_all_tables")

                statement.executeQuery().use { rs ->
                    if (rs.next()) {
                        return rs.getInt(1) >= 0
                    }
                }
                return false
            }
        } catch (e: SQLException) {
            LOG.error("Failed to connect to database", e)
            throw RuntimeException(e)
        }
    }

    override fun put(rowKey: String, columnName: String, value: String) {
        // Pay attention that in our model we may have arbitrary many columns, and they are not known in advance,
        // while the set of columns in relational tables is fixed in the table schema.
        //
        // In the minimal scope, it is enough to write code that only stores the values of the default column (thus
        // the data model essentially becomes a key-value, not a key-column-value).
        // You may opt into writing implementation that is capable of storing the values of any column and
        // get additional credits for that.
        TODO("Task 1")
    }

    override fun get(rowKey: String, columnName: String): String? {
        TODO("Task 1")
    }

    override fun getRow(rowKey: String): StoredRow? {
        TODO("Task 1")
    }

    override fun scan(conditions: Map<String, String>): RowScan {
        // Pay attention that storing our data in a database means that we can't afford keeping it in memory.
        // Thus, caching all the rows in this method implementation is not an option.
        // Take into account that it is possible that our clients will continue sending PUT requests while scan is in progress.
        TODO("Task 2")
    }

    override val supportedFeatures: Map<String, String> get() = emptyMap()

    override fun toString(): String {
        return "Database Storage ($config). Connected: $isReady"
    }
}

private val LOG = org.slf4j.LoggerFactory.getLogger("Storage.Database")
