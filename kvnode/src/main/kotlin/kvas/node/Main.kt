package kvas.node

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.optionalValue
import com.github.ajalt.clikt.parameters.types.int
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException
import io.grpc.ServerBuilder
import org.postgresql.util.PSQLException

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
  val master: String by option(help = "Master address in IP:PORT format").default("")
  val selfAddress_: String by option(help = "This node address in IP:PORT format", names = arrayOf("--self-address")).default("")
  val primary_: String? by option("--primary", help = "IP[:PORT] address of the leader host in a replica group").optionalValue("me")

  override fun run() {
    val selfAddress = if (selfAddress_ == "") {
      "127.0.0.1:$grpcPort"
    } else selfAddress_
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
      } catch (ex: PSQLException) {
        println("Postgres connection failed")
      }
    }
    if (master.isNotBlank()) {
      ServerBuilder.forPort(grpcPort).addService(KvasGrpcServerNode(selfAddress, master!!)).build().start().also {
        println("KVAS node started with self-address $selfAddress and master address $master. Hit Ctrl+C to stop")
        it.awaitTermination()
      }
    } else if (primary_?.isNotBlank() == true) {
      if (primary_ == "me") {
        ServerBuilder.forPort(grpcPort).addService(KvasReplicationLeader(selfAddress)).build().start().also {
          println("KVAS PRIMARY node started with self-address $selfAddress. Hit Ctrl+C to stop")
          it.awaitTermination()
        }
      } else {
        ServerBuilder.forPort(grpcPort).addService(KvasReplicationFollower(selfAddress, primary_!!)).build().start()
          .also {
            println("KVAS node started as a FOLLOWER with self-address $selfAddress and PRIMARY address $primary_. Hit Ctrl+C to stop")
            it.awaitTermination()
          }
      }
    } else {
      ServerBuilder.forPort(grpcPort).addService(KvasGrpcServerMaster(selfAddress)).build().start().also {
        println("KVAS master started with self-address $selfAddress. Hit Ctrl+C to stop")
        it.awaitTermination()
      }
    }
  }
}


fun main(args: Array<String>) = Main().main(args)
