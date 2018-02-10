package com.twitter.querulous.database

import java.sql.{Connection, DriverManager, SQLException}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executor, LinkedBlockingQueue, TimeUnit}

import com.twitter.querulous.driver.DatabaseDriver
import com.twitter.util.Duration
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.dbcp2.{DelegatingConnection, PoolingDataSource}
import org.apache.commons.pool2.ObjectPool

import scala.annotation.tailrec
import scala.compat.Platform

class PoolTimeoutException extends SQLException

class PoolEmptyException extends SQLException

class PooledConnection(c: Connection, p: ObjectPool[PooledConnection]) extends DelegatingConnection(c) {
  private var pool: Option[ObjectPool[PooledConnection]] = Some(p)

  private def invalidateConnection() = {
    pool.foreach(_.invalidateObject(this))
    pool = None
  }

  override def close(): Unit = {
    val closed = try {
      c.isClosed
    } catch {
      case e: Exception =>
        invalidateConnection()
        throw e
    }

    if (!closed) {
      pool match {
        case Some(pl) => pl.returnObject(this)
        case None =>
          passivate()
          c.close()
      }
    } else {
      invalidateConnection()
      throw new SQLException("Already closed.")
    }
  }

  private[database] def discard() = {
    invalidateConnection()
    try {
      c.close()
    } catch {
      case _: SQLException =>
    }
  }

  override def setSchema(schema: String): Unit = {}

  override def getNetworkTimeout: Int = 0

  override def getSchema: String = null

  override def setNetworkTimeout(executor: Executor, milliseconds: Int): Unit = {}

  override def abort(executor: Executor): Unit = {}
}

private[database] class ThrottledPool(
  factory: () => Connection,
  val size: Int,
  timeout: Duration,
  idleTimeout: Duration,
  name: String) extends ObjectPool[PooledConnection] with LazyLogging {

  private[this] val pool = new LinkedBlockingQueue[(PooledConnection, Long)]()
  private[this] val currentSize = new AtomicInteger(0)
  private[this] val numWaiters = new AtomicInteger(0)

  try {
    for (i <- 0 until size) addObject()
  } catch {
    // bail until the watchdog thread repopulates.
    case e: Throwable => logger.warn(s"Error initially populating pool $name", e)
  }

  override def addObject(): Unit = {
    pool.offer((new PooledConnection(factory(), this), Platform.currentTime))
    currentSize.incrementAndGet()
  }

  override final def borrowObject(): PooledConnection = {
    numWaiters.incrementAndGet()
    try {
      borrowObjectInternal()
    } finally {
      numWaiters.decrementAndGet()
    }
  }

  override def invalidateObject(obj: PooledConnection): Unit = {
    currentSize.decrementAndGet()
  }

  override def returnObject(obj: PooledConnection): Unit = {
    val conn = obj.asInstanceOf[PooledConnection]
    pool.offer((conn, Platform.currentTime))
  }

  override def clear(): Unit = pool.clear()

  override def close(): Unit = pool.clear()

  override def getNumActive: Int = currentSize.get() - pool.size()

  override def getNumIdle: Int = pool.size()

  def getTotal: Int = currentSize.get()

  def getNumWaiters: Int = numWaiters.get()

  def addObjectUnlessFull(): Unit = synchronized {
    if (getTotal < size) {
      addObject()
    }
  }

  def addObjectIfEmpty(): Unit = synchronized {
    if (getTotal == 0) addObject()
  }

  @tailrec
  private def borrowObjectInternal(): PooledConnection = {
    // short circuit if the pool is empty
    if (getTotal == 0) throw new PoolEmptyException

    val pair = pool.poll(timeout.inMilliseconds, TimeUnit.MILLISECONDS)
    if (pair == null) throw new PoolTimeoutException
    val (connection, lastUse) = pair

    if ((Platform.currentTime - lastUse) > idleTimeout.inMilliseconds) {
      // TODO: perhaps replace with forcible termination.
      try {
        connection.discard()
      } catch {
        case _: SQLException =>
      }
      // Note: dbcp handles object invalidation here.
      addObjectIfEmpty()
      borrowObjectInternal()
    } else {
      connection
    }
  }
}

/**
 *
 * @param pool
 * @param hosts
 * @param repopulateInterval
 * @todo provide a reliable way to have this thread exit when shutdown is implemented
 */
private[database] class PoolWatchdogThread(pool: ThrottledPool, hosts: Seq[String], repopulateInterval: Duration)
  extends Thread(hosts.mkString(",") + "-pool-watchdog") with LazyLogging {

  setDaemon(true)

  override def run(): Unit = {
    var lastTimePoolPopulated = Platform.currentTime
    while (true) {
      try {
        val timeToSleepInMills = repopulateInterval.inMilliseconds - (Platform.currentTime - lastTimePoolPopulated)
        if (timeToSleepInMills > 0) {
          Thread.sleep(timeToSleepInMills)
        }
        lastTimePoolPopulated = Platform.currentTime
        pool.addObjectUnlessFull()
      } catch {
        case t: Throwable => reportException(t)
      }
    }
  }

  def reportException(t: Throwable): Unit = {
    val thread = Thread.currentThread().getName
    val errMsg = "%s: %s" format(t.getClass.getName, t.getMessage)
    logger.warn("%s: Failed to add connection to the pool: %s" format(thread, errMsg), t)
  }
}

class ThrottledPoolingDatabaseFactory(
  serviceName: Option[String],
  size: Int,
  openTimeout: Duration,
  idleTimeout: Duration,
  repopulateInterval: Duration,
  defaultUrlOptions: Map[String, String]) extends DatabaseFactory {

  def this(
    size: Int, openTimeout: Duration, idleTimeout: Duration, repopulateInterval: Duration,
    defaultUrlOptions: Map[String, String]) = {
    this(None, size, openTimeout, idleTimeout, repopulateInterval, defaultUrlOptions)
  }

  def this(
    size: Int, openTimeout: Duration, idleTimeout: Duration,
    repopulateInterval: Duration) = {
    this(size, openTimeout, idleTimeout, repopulateInterval, Map.empty)
  }

  def apply(dbhosts: Seq[String], dbname: String, username: String, password: String, urlOptions: Map[String, String], driver: DatabaseDriver): Database = {
    val finalUrlOptions =
      if (urlOptions eq null) {
        defaultUrlOptions
      } else {
        defaultUrlOptions ++ urlOptions
      }

    new ThrottledPoolingDatabase(serviceName, dbhosts, dbname, username, password, finalUrlOptions,
      driver, size, openTimeout, idleTimeout, repopulateInterval)
  }
}

class ThrottledPoolingDatabase(
  val serviceName: Option[String],
  val hosts: Seq[String],
  val name: String,
  val username: String,
  password: String,
  val extraUrlOptions: Map[String, String],
  val driver: DatabaseDriver,
  numConnections: Int,
  val openTimeout: Duration,
  idleTimeout: Duration,
  repopulateInterval: Duration)
  extends Database {

  driver.load()

  private val pool = new ThrottledPool(mkConnection, numConnections, openTimeout, idleTimeout, hosts.mkString(","))
  private val poolingDataSource = new PoolingDataSource(pool)
  poolingDataSource.setAccessToUnderlyingConnectionAllowed(true)

  new PoolWatchdogThread(pool, hosts, repopulateInterval).start()

  private val gaugePrefix = serviceName.map(_ + "-").getOrElse("")

  private val gauges =
    if (gaugePrefix.nonEmpty) {
      Seq((gaugePrefix + hosts.mkString(",") + "-num-connections", () => pool.getTotal.toDouble),
        (gaugePrefix + hosts.mkString(",") + "-num-idle-connections", () => pool.getNumIdle.toDouble),
        (gaugePrefix + hosts.mkString(",") + "-num-waiters", () => pool.getNumWaiters.toDouble))
    } else {
      Seq.empty
    }

  def this(
    hosts: Seq[String], name: String, username: String, password: String,
    extraUrlOptions: Map[String, String], numConnections: Int, openTimeout: Duration,
    idleTimeout: Duration, repopulateInterval: Duration) = {
    this(None, hosts, name, username, password, extraUrlOptions, Database.DefaultDriver, numConnections, openTimeout,
      idleTimeout, repopulateInterval)
  }

  override def open(): Connection = {
    try {
      poolingDataSource.getConnection
    } catch {
      case e: PoolTimeoutException =>
        throw new SqlDatabaseTimeoutException(hosts.mkString(",") + "/" + name, openTimeout)
    }
  }

  override def close(connection: Connection): Unit = {
    try {
      connection.close()
    } catch {
      case _: SQLException =>
    }
  }


  override def shutdown(): Unit = pool.close()

  protected def mkConnection(): Connection =
    DriverManager.getConnection(url(hosts, name, urlOptions), username, password)

  override protected[database] def getGauges: Seq[(String, () => Double)] = gauges
}