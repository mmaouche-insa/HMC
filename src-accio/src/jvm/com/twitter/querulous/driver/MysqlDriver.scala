package com.twitter.querulous.driver

import java.sql.Connection

class MysqlDriver extends DatabaseDriver {
  private[this] lazy val connClass = Class.forName("MySQLConnection")

  override val name = "mysql"

  override def defaultUrlOptions: Map[String, String] = Map(
    "useUnicode" -> "true",
    "characterEncoding" -> "UTF-8",
    "connectTimeout" -> "100"
  )

  override def abort(conn: Connection): Boolean = {
    try {
      connClass.getClass.getMethod("abortInternal").invoke(conn)
      true
    } catch {
      case _: Throwable => false
    }
  }

  override protected def driverClassName: String = "com.mysql.jdbc.Driver"
}
