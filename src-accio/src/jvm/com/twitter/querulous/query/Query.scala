package com.twitter.querulous.query

import java.sql.{Connection, ResultSet}

import scala.collection.immutable.Map
import scala.collection.mutable

trait QueryFactory {
  def apply(connection: Connection, queryClass: QueryClass, queryString: String, params: Any*): Query

  def shutdown(): Unit = {}
}

trait Query {
  private val ann = mutable.Map[String, String]()

  def select[A](f: ResultSet => A): Seq[A]

  def execute(): Int

  def addParams(params: Any*): Unit

  /**
   * Cancels the execution of this query.
   */
  def cancel(): Unit

  /**
   * Adds an annotation you want to be sent along with the query as a comment. Could for example be information you want
   * to find in a slow query log.
   *
   * @param key An annotation key
   * @param value An annotation value
   */
  def annotate(key: String, value: String): Unit = ann += (key -> value)

  /**
   * Returns the list of annotations attached to this query.
   *
   * @return A map of annotations (key -> value)
   */
  def annotations: Map[String, String] = ann.toMap
}