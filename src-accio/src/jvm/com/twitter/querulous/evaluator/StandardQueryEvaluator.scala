package com.twitter.querulous.evaluator

import java.sql.ResultSet

import com.twitter.querulous.database.{Database, DatabaseFactory}
import com.twitter.querulous.driver.DatabaseDriver
import com.twitter.querulous.query.{QueryClass, QueryFactory}

class StandardQueryEvaluatorFactory(
  databaseFactory: DatabaseFactory,
  queryFactory: QueryFactory) extends QueryEvaluatorFactory {

  def apply(dbhosts: Seq[String], dbname: String, username: String, password: String, urlOptions: Map[String, String], driver: DatabaseDriver) = {
    val database = databaseFactory(dbhosts, dbname, username, password, urlOptions, driver)
    new StandardQueryEvaluator(database, queryFactory)
  }
}

class StandardQueryEvaluator(protected val database: Database, queryFactory: QueryFactory)
  extends QueryEvaluator {

  def select[A](queryClass: QueryClass, query: String, params: Any*)(f: ResultSet => A) = {
    withTransaction(_.select(queryClass, query, params: _*)(f))
  }

  def selectOne[A](queryClass: QueryClass, query: String, params: Any*)(f: ResultSet => A) = {
    withTransaction(_.selectOne(queryClass, query, params: _*)(f))
  }

  def count(queryClass: QueryClass, query: String, params: Any*) = {
    withTransaction(_.count(queryClass, query, params: _*))
  }

  def execute(queryClass: QueryClass, query: String, params: Any*) = {
    withTransaction(_.execute(queryClass, query, params: _*))
  }

  def executeBatch(queryClass: QueryClass, query: String)(f: ParamsApplier => Unit) = {
    withTransaction(_.executeBatch(queryClass, query)(f))
  }

  def insert(queryClass: QueryClass, query: String, params: Any*) = {
    withTransaction(_.insert(queryClass, query, params: _*))
  }

  def transaction[T](f: Transaction => T) = {
    withTransaction { transaction =>
      transaction.begin()
      try {
        val rv = f(transaction)
        transaction.commit()
        rv
      } catch {
        case e: Throwable =>
          try {
            transaction.rollback()
          } catch {
            case _: Throwable => ()
          }
          throw e
      }
    }
  }

  def shutdown() {
    database.shutdown()
  }

  private def withTransaction[A](f: Transaction => A) = {
    database.withConnection { connection => f(new Transaction(queryFactory, connection)) }
  }

  override def equals(other: Any) = {
    other match {
      case other: StandardQueryEvaluator =>
        database eq other.database
      case _ =>
        false
    }
  }

  override def hashCode = database.hashCode
}
