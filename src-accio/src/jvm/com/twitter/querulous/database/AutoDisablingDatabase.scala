package com.twitter.querulous.database

import java.sql.{Connection, SQLException}

import com.twitter.querulous.AutoDisabler
import com.twitter.querulous.driver.DatabaseDriver
import com.twitter.util.Duration

class AutoDisablingDatabaseFactory(databaseFactory: DatabaseFactory, disableErrorCount: Int, disableDuration: Duration) extends DatabaseFactory {
  def apply(dbhosts: Seq[String], dbname: String, username: String, password: String, urlOptions: Map[String, String], driver: DatabaseDriver): Database =
    new AutoDisablingDatabase(
      databaseFactory(dbhosts, dbname, username, password, urlOptions, driver),
      disableErrorCount,
      disableDuration)
}

class AutoDisablingDatabase(
  val database: Database,
  protected val disableErrorCount: Int,
  protected val disableDuration: Duration)
  extends Database
    with DatabaseProxy
    with AutoDisabler {
  override def open(): Connection = {
    throwIfDisabled(database.hosts.head)
    try {
      val rv = database.open()
      noteOperationOutcome(true)
      rv
    } catch {
      case e: SQLException =>
        noteOperationOutcome(false)
        throw e
      case e: Exception =>
        throw e
    }
  }

  override def close(connection: Connection): Unit = database.close(connection)

  override def shutdown(): Unit = database.shutdown()
}
