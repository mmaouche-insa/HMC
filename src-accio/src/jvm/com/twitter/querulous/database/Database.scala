package com.twitter.querulous.database

import java.sql.Connection

import com.twitter.querulous.driver.DatabaseDriver
import com.twitter.util.Duration

object Database {
  val DefaultDriver = DatabaseDriver.MySql
}

trait DatabaseFactory {
  def apply(dbhosts: Seq[String], dbname: String, username: String, password: String, urlOptions: Map[String, String], driver: DatabaseDriver): Database

  def apply(dbhosts: Seq[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]): Database =
    apply(dbhosts, dbname, username, password, urlOptions, Database.DefaultDriver)

  def apply(driver: DatabaseDriver, dbhosts: Seq[String], dbname: String, username: String, password: String): Database =
    apply(dbhosts, dbname, username, password, Map.empty, driver)

  def apply(dbhosts: Seq[String], dbname: String, username: String, password: String): Database =
    apply(dbhosts, dbname, username, password, Map.empty, Database.DefaultDriver)

  def apply(dbhosts: Seq[String], username: String, password: String): Database =
    apply(dbhosts, null, username, password, Map.empty, Database.DefaultDriver)

  def apply(driver: DatabaseDriver, dbhosts: Seq[String], username: String, password: String): Database =
    apply(dbhosts, null, username, password, Map.empty, driver)
}

trait Database {
  def driver: DatabaseDriver

  def hosts: Seq[String]

  def name: String

  def username: String

  def extraUrlOptions: Map[String, String]

  def openTimeout: Duration

  def open(): Connection

  def close(connection: Connection): Unit

  def shutdown(): Unit

  def urlOptions: Map[String, String] = driver.defaultUrlOptions ++ extraUrlOptions

  def withConnection[A](f: Connection => A): A = {
    val connection = open()
    try {
      f(connection)
    } finally {
      close(connection)
    }
  }

  protected[database] def getGauges: Seq[(String, () => Double)] = List.empty

  protected def url(hosts: Seq[String], name: String, urlOptions: Map[String, String]) = {
    val nameSegment = if (name == null) "" else "/" + name
    val urlOptsSegment = urlOptions.map(Function.tupled((k, v) => k + "=" + v)).mkString("&")

    "jdbc:" + driver.name + "://" + hosts.mkString(",") + nameSegment + "?" + urlOptsSegment
  }
}

trait DatabaseProxy extends Database {
  def database: Database

  override def driver: DatabaseDriver = database.driver

  override def hosts: Seq[String] = database.hosts

  override def name: String = database.name

  override def username: String = database.username

  override def extraUrlOptions: Map[String, String] = database.extraUrlOptions

  override def openTimeout: Duration = database.openTimeout

  def innermostDatabase: Database = {
    database match {
      case dbProxy: DatabaseProxy => dbProxy.innermostDatabase
      case db: Database => db
    }
  }
}
