package com.twitter.querulous.evaluator

import java.sql.ResultSet

import com.twitter.querulous._
import com.twitter.querulous.database._
import com.twitter.querulous.driver.DatabaseDriver
import com.twitter.querulous.query._

import com.twitter.conversions.time._

object QueryEvaluator extends QueryEvaluatorFactory {
  private def createEvaluatorFactory() = {
    val queryFactory = new SqlQueryFactory
    val databaseFactory = new ApachePoolingDatabaseFactory(10, 10, 1.second, 10.millis, false, 0.seconds)
    new StandardQueryEvaluatorFactory(databaseFactory, queryFactory)
  }

  def apply(dbhosts: Seq[String], dbname: String, username: String, password: String, urlOptions: Map[String, String], driver: DatabaseDriver): QueryEvaluator =
    createEvaluatorFactory()(dbhosts, dbname, username, password, urlOptions, driver)
}

trait QueryEvaluatorFactory {
  def apply(dbhosts: Seq[String], dbname: String, username: String, password: String, urlOptions: Map[String, String], driver: DatabaseDriver): QueryEvaluator

  def apply(dbhosts: Seq[String], dbname: String, username: String, password: String, urlOptions: Map[String, String]): QueryEvaluator =
    apply(dbhosts, dbname, username, password, urlOptions, Database.DefaultDriver)

  def apply(dbhosts: Seq[String], dbname: String, username: String, password: String): QueryEvaluator =
    apply(dbhosts, dbname, username, password, Map.empty, Database.DefaultDriver)

  def apply(dbhosts: Seq[String], dbname: String, username: String, password: String, driver: DatabaseDriver): QueryEvaluator =
    apply(dbhosts, dbname, username, password, Map.empty, driver)

  def apply(dbhosts: Seq[String], username: String, password: String, urlOptions: Map[String, String]): QueryEvaluator =
    apply(dbhosts, null, username, password, urlOptions, Database.DefaultDriver)

  def apply(dbhosts: Seq[String], username: String, password: String): QueryEvaluator =
    apply(dbhosts, null, username, password, Map.empty, Database.DefaultDriver)

  def apply(dbhosts: Seq[String], username: String, password: String, driver: DatabaseDriver): QueryEvaluator =
    apply(dbhosts, null, username, password, Map.empty, driver)

  def apply(connection: config.Connection): QueryEvaluator =
    apply(connection.hostnames.toList, connection.database, connection.username, connection.password, connection.urlOptions, DatabaseDriver(connection.driver))
}

class ParamsApplier(query: Query) {
  def apply(params: Any*): Unit = query.addParams(params: _*)
}

trait QueryEvaluator {
  def select[A](queryClass: QueryClass, query: String, params: Any*)(f: ResultSet => A): Seq[A]

  def select[A](query: String, params: Any*)(f: ResultSet => A): Seq[A] =
    select(QueryClass.Select, query, params: _*)(f)

  def selectOne[A](queryClass: QueryClass, query: String, params: Any*)(f: ResultSet => A): Option[A]

  def selectOne[A](query: String, params: Any*)(f: ResultSet => A): Option[A] =
    selectOne(QueryClass.Select, query, params: _*)(f)

  def count(queryClass: QueryClass, query: String, params: Any*): Int

  def count(query: String, params: Any*): Int = count(QueryClass.Select, query, params: _*)

  def execute(queryClass: QueryClass, query: String, params: Any*): Int

  def execute(query: String, params: Any*): Int = execute(QueryClass.Execute, query, params: _*)

  def executeBatch(queryClass: QueryClass, query: String)(f: ParamsApplier => Unit): Int

  def executeBatch(query: String)(f: ParamsApplier => Unit): Int = executeBatch(QueryClass.Execute, query)(f)

  def insert(queryClass: QueryClass, query: String, params: Any*): Long

  def insert(query: String, params: Any*): Long = insert(QueryClass.Execute, query, params: _*)

  def transaction[T](f: Transaction => T): T

  def shutdown(): Unit
}
