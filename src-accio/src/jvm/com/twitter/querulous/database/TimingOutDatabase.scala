package com.twitter.querulous.database

import java.sql.{Connection, SQLException}

import com.twitter.querulous.driver.DatabaseDriver
import com.twitter.querulous.{FutureTimeout, TimeoutException}
import com.twitter.util.Duration

class TimingOutDatabaseFactory(
  databaseFactory: DatabaseFactory,
  poolSize: Int,
  queueSize: Int,
  openTimeout: Duration)
  extends DatabaseFactory {

  def apply(dbhosts: Seq[String], dbname: String, username: String, password: String, urlOptions: Map[String, String], driver: DatabaseDriver): Database =
    new TimingOutDatabase(
      databaseFactory(dbhosts, dbname, username, password, urlOptions, driver),
      newTimeoutPool(),
      openTimeout
    )

  private def newTimeoutPool() = new FutureTimeout(poolSize, queueSize)
}

/**
 * Implementation of a database where there is a maximum amount of time allowed when establishing the connection.
 *
 * @param database    A wrapped database
 * @param timeout
 * @param openTimeout Timeout when connecting
 */
class TimingOutDatabase(val database: Database, timeout: FutureTimeout, openTimeout: Duration) extends Database with DatabaseProxy {
  override def open(): Connection = getConnection(openTimeout)

  override def close(connection: Connection): Unit = database.close(connection)

  override def shutdown(): Unit = database.shutdown()

  private def getConnection(wait: Duration) =
    try {
      timeout(wait) {
        database.open()
      } { conn =>
        database.close(conn)
      }
    } catch {
      case e: TimeoutException =>
        throw new SqlDatabaseTimeoutException(label, wait)
    }

  private def label = database.name match {
    case null => database.hosts.mkString(",") + "/ (null)"
    case name => database.hosts.mkString(",") + "/" + name
  }
}

/**
 * Exception thrown if a timeout occurs when connecting to the database.
 *
 * @param msg     A message
 * @param timeout Timeout duration
 */
class SqlDatabaseTimeoutException(msg: String, val timeout: Duration) extends SQLException(msg)
