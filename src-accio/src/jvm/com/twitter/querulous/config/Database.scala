package com.twitter.querulous.config

import com.twitter.conversions.time._
import com.twitter.querulous._
import com.twitter.querulous.database._
import com.twitter.util.Duration

trait PoolingDatabase {
  def apply(): DatabaseFactory
}

trait ServiceNameTagged {
  def apply(serviceName: Option[String]): DatabaseFactory
}

class ApachePoolingDatabase extends PoolingDatabase {
  var sizeMin: Int = 10
  var sizeMax: Int = 10
  var testIdle: Duration = 1 second
  var maxWait: Duration = 10 millis
  var minEvictableIdle: Duration = 60.seconds
  var testOnBorrow: Boolean = false

  def apply(): DatabaseFactory =
    new ApachePoolingDatabaseFactory(sizeMin, sizeMax, testIdle, maxWait, testOnBorrow, minEvictableIdle)
}

class ThrottledPoolingDatabase extends PoolingDatabase with ServiceNameTagged {
  var size: Int = 10
  var openTimeout: Duration = 50 millis
  var repopulateInterval: Duration = 500 millis
  var idleTimeout: Duration = 1 minute

  def apply(): DatabaseFactory = apply(None)

  def apply(serviceName: Option[String]): DatabaseFactory =
    new ThrottledPoolingDatabaseFactory(serviceName, size, openTimeout, idleTimeout, repopulateInterval, Map.empty)
}

class TimingOutDatabase {
  var poolSize: Int = 10
  var queueSize: Int = 10000
  var open: Duration = 1 second

  def apply(factory: DatabaseFactory): DatabaseFactory = new TimingOutDatabaseFactory(factory, poolSize, queueSize, open)
}

trait AutoDisablingDatabase {
  def errorCount: Int

  def interval: Duration

  def apply(factory: DatabaseFactory): DatabaseFactory = new AutoDisablingDatabaseFactory(factory, errorCount, interval)
}

class Database {
  var pool: Option[PoolingDatabase] = None
  var autoDisable: Option[AutoDisablingDatabase] = None
  var timeout: Option[TimingOutDatabase] = None
  var memoize: Boolean = true
  var serviceName: Option[String] = None

  def pool_=(p: PoolingDatabase): Unit = {
    pool = Some(p)
  }

  def autoDisable_=(a: AutoDisablingDatabase): Unit = {
    autoDisable = Some(a)
  }

  def timeout_=(t: TimingOutDatabase): Unit = {
    timeout = Some(t)
  }

  def serviceName_=(s: String): Unit = {
    serviceName = Some(s)
  }

  def apply(): DatabaseFactory = apply(NullStatsCollector)

  def apply(stats: StatsCollector): DatabaseFactory = apply(stats, None)

  def apply(stats: StatsCollector, statsFactory: DatabaseFactory => DatabaseFactory): DatabaseFactory =
    apply(stats, Some(statsFactory))

  def apply(stats: StatsCollector, statsFactory: Option[DatabaseFactory => DatabaseFactory]): DatabaseFactory = {
    var factory = pool.map {
      case p: ServiceNameTagged => p(serviceName)
      case p: PoolingDatabase => p()
    }.getOrElse(new SingleConnectionDatabaseFactory)

    timeout.foreach(timeout => factory = timeout(factory))

    statsFactory.foreach(f => factory = f(factory))

    if (stats ne NullStatsCollector) {
      factory = new StatsCollectingDatabaseFactory(factory, stats)
    }

    autoDisable.foreach(autoDisable => factory = autoDisable(factory))

    if (memoize) {
      factory = new MemoizingDatabaseFactory(factory)
    }

    factory
  }
}

trait Connection {
  def hostnames: Seq[String]

  def database: String

  def username: String

  def password: String

  var urlOptions: Map[String, String] = Map()

  var driver: String = Database.DefaultDriver.name

  def withHost(newHost: String) = {
    val current = this
    new Connection {
      def hostnames = Seq(newHost)

      def database = current.database

      def username = current.username

      def password = current.password

      urlOptions = current.urlOptions
      driver = current.driver
    }
  }

  def withHosts(newHosts: Seq[String]) = {
    val current = this
    new Connection {
      def hostnames = newHosts

      def database = current.database

      def username = current.username

      def password = current.password

      urlOptions = current.urlOptions

      driver = current.driver
    }
  }

  def withDatabase(newDatabase: String) = {
    val current = this
    new Connection {
      def hostnames = current.hostnames

      def database = newDatabase

      def username = current.username

      def password = current.password

      urlOptions = current.urlOptions

      driver = current.driver
    }
  }

  def withoutDatabase = {
    val current = this
    new Connection {
      def hostnames = current.hostnames

      def database = null

      def username = current.username

      def password = current.password

      urlOptions = current.urlOptions

      driver = current.driver
    }
  }
}
