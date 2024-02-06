package kvas.node

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException
import io.grpc.ServerBuilder

/**
 * Точка входа для квасного сервера. Разбирает аргументы командной строки, устанавливает соединение с постгресом,
 * создает GRPC сервер.
 */
class Main : CliktCommand() {
  val dbHost: String by option(help = "Database host name").default("localhost")
  val dbPort: Int by option(help = "Database port number").int().default(5432)
  val dbUser: String by option(help = "Database user name").default("postgres")
  val dbPassword: String by option(help = "Database user password").default("")
  val dbDatabase: String by option(help = "Database name").default("")

  val grpcPort: Int by option(help = "Kvas GRPC port number").int().default(9000)
  override fun run() {
    if (dbHost != "none") {
      val hikariConfig = HikariConfig().apply {
        username = dbUser
        password = dbPassword
        jdbcUrl = "jdbc:postgresql://${dbHost}:${dbPort}/${dbDatabase.ifEmpty { dbUser }}"
        addDataSourceProperty("ssl.mode", "disable");
      }

      try {
        PGStorage(HikariDataSource(hikariConfig)).testConnection().let {
          if (it) {
            println("Successfully connected to Postgres")
          } else {
            println("Postgres connection failed")
          }
        }
      } catch (ex: PoolInitializationException) {
        println("Postgres connection failed")
      }
    }
    ServerBuilder.forPort(grpcPort).addService(KvasGrpcServer()).build().start().also {
      println("GRPC Server started. Hit Ctrl+C to stop")
      it.awaitTermination()
    }
  }
}


fun main(args: Array<String>) = Main().main(args)
