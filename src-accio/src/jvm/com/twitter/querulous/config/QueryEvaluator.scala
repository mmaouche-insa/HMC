package com.twitter.querulous.config

import com.twitter.querulous._
import com.twitter.querulous.database.DatabaseFactory
import com.twitter.querulous.evaluator._
import com.twitter.querulous.query.QueryFactory
import com.twitter.util.Duration

trait AutoDisablingQueryEvaluator {
  def errorCount: Int

  def interval: Duration
}

class QueryEvaluator {
  private var memoizedFactory: Option[QueryEvaluatorFactory] = None
  var database: Database = new Database
  var query: Query = new Query
  var singletonFactory = false
  var autoDisable: Option[AutoDisablingQueryEvaluator] = None

  def autoDisable_=(a: AutoDisablingQueryEvaluator): Unit = {
    autoDisable = Some(a)
  }

  def apply(): QueryEvaluatorFactory = apply(NullStatsCollector)

  def apply(stats: StatsCollector): QueryEvaluatorFactory = apply(stats, None, None)

  def apply(stats: StatsCollector, dbStatsFactory: DatabaseFactory => DatabaseFactory, queryStatsFactory: QueryFactory => QueryFactory): QueryEvaluatorFactory =
    apply(stats, Some(dbStatsFactory), Some(queryStatsFactory))

  def apply(stats: StatsCollector, dbStatsFactory: Option[DatabaseFactory => DatabaseFactory], queryStatsFactory: Option[QueryFactory => QueryFactory]): QueryEvaluatorFactory = synchronized {
    if (!singletonFactory) {
      memoizedFactory = None
    }
    memoizedFactory = memoizedFactory orElse {
      var factory: QueryEvaluatorFactory = new StandardQueryEvaluatorFactory(database(stats, dbStatsFactory), query(stats, queryStatsFactory))
      autoDisable.foreach { disable =>
        factory = new AutoDisablingQueryEvaluatorFactory(
          factory, disable.errorCount, disable.interval
        )
      }
      Some(factory)
    }
    memoizedFactory.get
  }
}