package com.twitter.querulous.config

import com.twitter.conversions.time._
import com.twitter.querulous._
import com.twitter.querulous.query._
import com.twitter.util.Duration
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable

object QueryTimeout {
  def apply(timeout: Duration, cancelOnTimeout: Boolean): QueryTimeout = new QueryTimeout(timeout, cancelOnTimeout)

  def apply(timeout: Duration): QueryTimeout = new QueryTimeout(timeout, false)
}

class QueryTimeout(val timeout: Duration, val cancelOnTimeout: Boolean)

object NoDebugOutput extends (String => Unit) {
  def apply(s: String): Unit = ()
}

class Query extends LazyLogging {
  var timeouts: mutable.Map[QueryClass, QueryTimeout] = mutable.Map(
    QueryClass.Select -> QueryTimeout(5.seconds),
    QueryClass.Execute -> QueryTimeout(5.seconds)
  )
  var retries: Int = 0
  var debug: (String => Unit) = NoDebugOutput

  def apply(statsCollector: StatsCollector): QueryFactory = apply(statsCollector, None)

  def apply(statsCollector: StatsCollector, statsFactory: QueryFactory => QueryFactory): QueryFactory = apply(statsCollector, Some(statsFactory))

  def apply(statsCollector: StatsCollector, statsFactory: Option[QueryFactory => QueryFactory]): QueryFactory = {
    var queryFactory: QueryFactory = new SqlQueryFactory

    if (timeouts.nonEmpty) {
      val tupleTimeout = timeouts.map { case (queryClass, timeout) =>
        queryClass ->(timeout.timeout, timeout.cancelOnTimeout)
      }.toMap
      queryFactory = new PerQueryTimingOutQueryFactory(queryFactory, tupleTimeout)
      logger.debug(s"Query timeout: ${timeouts.map { case (k, v) => k.name + ": " + v.timeout }.mkString(", ")}")
    }

    statsFactory.foreach { f =>
      queryFactory = f(queryFactory)
    }

    if (statsCollector ne NullStatsCollector) {
      queryFactory = new StatsCollectingQueryFactory(queryFactory, statsCollector)
    }

    if (retries > 0) {
      queryFactory = new RetryingQueryFactory(queryFactory, retries)
      logger.debug(s"Query retries: $retries")
    }

    if (debug ne NoDebugOutput) {
      queryFactory = new DebuggingQueryFactory(queryFactory, debug)
      logger.debug(s"Query debug: $debug")
    }

    queryFactory
  }

  def apply(): QueryFactory = apply(NullStatsCollector)
}
