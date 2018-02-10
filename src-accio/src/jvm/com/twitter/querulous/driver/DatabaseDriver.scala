package com.twitter.querulous.driver

import java.sql.Connection
import java.util.concurrent.atomic.AtomicBoolean

trait DatabaseDriver {
  private val loaded = new AtomicBoolean(false)

  def name: String

  def defaultUrlOptions: Map[String, String]

  def abort(conn: Connection): Boolean = false

  final def load(): Unit =
    if (loaded.compareAndSet(false, true)) {
      Class.forName(driverClassName)
    }

  protected def driverClassName: String
}

object DatabaseDriver {
  val MySql = new MysqlDriver
  val PgSql = new PgsqlDriver

  def apply(name: String): DatabaseDriver = name match {
    case "mysql" => MySql
    case "postgresql" => PgSql
    case _ => throw new IllegalArgumentException(s"Unknown database driver $name.")
  }
}