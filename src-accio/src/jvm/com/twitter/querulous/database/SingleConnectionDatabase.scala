package com.twitter.querulous.database

import java.sql.{Connection, SQLException}

import com.twitter.conversions.time._
import com.twitter.querulous.driver.DatabaseDriver
import org.apache.commons.dbcp2.DriverManagerConnectionFactory

class SingleConnectionDatabaseFactory(defaultUrlOptions: Map[String, String]) extends DatabaseFactory {
  def this() = this(Map.empty)

  def apply(dbhosts: Seq[String], dbname: String, username: String, password: String, urlOptions: Map[String, String], driver: DatabaseDriver): Database = {
    val finalUrlOptions =
      if (urlOptions eq null) {
        defaultUrlOptions
      } else {
        defaultUrlOptions ++ urlOptions
      }

    new SingleConnectionDatabase(dbhosts, dbname, username, password, finalUrlOptions, driver)
  }
}

class SingleConnectionDatabase(
  val hosts: Seq[String],
  val name: String,
  val username: String,
  password: String,
  val extraUrlOptions: Map[String, String],
  val driver: DatabaseDriver)
  extends Database {

  driver.load()
  private val connectionFactory = new DriverManagerConnectionFactory(url(hosts, name, urlOptions), username, password)
  val openTimeout = urlOptions("connectTimeout").toInt.millis

  override def close(connection: Connection): Unit =
    try {
      connection.close()
    } catch {
      case _: SQLException =>
    }

  override def shutdown(): Unit = {}

  override def open(): Connection = connectionFactory.createConnection()

  override def toString: String = hosts.head + "_" + name
}
