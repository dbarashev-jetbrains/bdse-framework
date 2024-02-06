package kvas.node

import java.sql.SQLException
import javax.sql.DataSource


/**
 * Код для общения с постгресом, используемым в качестве персистентного хранилища.
 */
class PGStorage(private val dataSource: DataSource) {
  fun testConnection() : Boolean {
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
      e.printStackTrace()
      throw RuntimeException(e)
    }
  }
}
