package com.twitter.querulous.evaluator

import java.sql.{SQLException, SQLIntegrityConstraintViolationException}

import com.twitter.querulous.AutoDisabler
import com.twitter.querulous.driver.DatabaseDriver
import com.twitter.util.Duration

class AutoDisablingQueryEvaluatorFactory(
  queryEvaluatorFactory: QueryEvaluatorFactory,
  disableErrorCount: Int,
  disableDuration: Duration) extends QueryEvaluatorFactory {

  private def chainEvaluator(evaluator: QueryEvaluator) =
    new AutoDisablingQueryEvaluator(evaluator, disableErrorCount, disableDuration)

  def apply(dbhosts: Seq[String], dbname: String, username: String, password: String, urlOptions: Map[String, String], driver: DatabaseDriver): QueryEvaluator = {
    chainEvaluator(queryEvaluatorFactory(dbhosts, dbname, username, password, urlOptions, driver))
  }
}

class AutoDisablingQueryEvaluator(
  queryEvaluator: QueryEvaluator,
  protected val disableErrorCount: Int,
  protected val disableDuration: Duration) extends QueryEvaluatorProxy(queryEvaluator) with AutoDisabler {

  override protected def delegate[A](f: => A) = {
    throwIfDisabled()
    try {
      val rv = f
      noteOperationOutcome(true)
      rv
    } catch {
      /*case e: MySQLIntegrityConstraintViolationException =>
        // user error: don't blame the db.
        throw e*/
      case e: SQLIntegrityConstraintViolationException =>
        // user error: don't blame the db.
        throw e
      case e: SQLException =>
        noteOperationOutcome(false)
        throw e
      case e: Exception =>
        throw e
    }
  }

  override def shutdown(): Unit = {
    queryEvaluator.shutdown()
  }
}
