package com.twitter.querulous.query

import java.sql.{Connection, Timestamp}

class DebuggingQueryFactory(queryFactory: QueryFactory, log: String => Unit) extends QueryFactory {
  def apply(connection: Connection, queryClass: QueryClass, query: String, params: Any*) =
    new DebuggingQuery(queryFactory(connection, queryClass, query, params: _*), log, query, params)
}

class DebuggingQuery(query: Query, log: String => Unit, queryString: String, params: Seq[Any]) extends QueryProxy(query) {
  override def delegate[A](f: => A) = {
    log(queryString + " " + params.map(makeDebugString).mkString("(", ", ", ")"))
    f
  }

  private def makeDebugString(param: Any): String = {
    param match {
      case s: String =>
        "\"" + s + "\""
      case c: Char =>
        "'" + c + "'"
      case l: Long =>
        l.toString
      case i: Int =>
        i.toString
      case b: Array[Byte] =>
        "(" + b.length + " bytes)"
      case b: Boolean =>
        b.toString
      case d: Double =>
        d.toString
      case t: Timestamp =>
        t.toString
      case is: Iterable[_] =>
        is.map(makeDebugString).mkString("(", ", ", ")")
      case nv: NullValue =>
        "null"
      case _ =>
        "Unknown argument type."
    }
  }
}
