package com.twitter.querulous.driver

import java.sql.Connection
import java.util.concurrent.Executors

class PgsqlDriver extends DatabaseDriver {
  private[this] lazy val conn4Class = Class.forName("org.postgresql.jdbc4.Jdbc4Connection")

  override val name = "postgresql"

  override def defaultUrlOptions: Map[String, String] = Map(
    "connectTimeout" -> "100"
  )

  override def abort(conn: Connection): Boolean = {
    if (conn4Class.getClass.isAssignableFrom(conn.getClass)) {
      try {
        conn4Class.getClass.getMethod("abort").invoke(conn, Executors.newSingleThreadExecutor)
        true
      } catch {
        case _: Throwable => false
      }
    } else false
  }

  override protected def driverClassName: String = "org.postgresql.Driver"
}