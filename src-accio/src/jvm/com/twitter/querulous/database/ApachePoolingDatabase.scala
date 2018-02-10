package com.twitter.querulous.database

import java.sql.{Connection, SQLException}

import com.twitter.querulous.driver.DatabaseDriver
import com.twitter.util.Duration
import org.apache.commons.dbcp2.{DriverManagerConnectionFactory, PoolableConnection, PoolableConnectionFactory, PoolingDataSource}
import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}

class ApachePoolingDatabaseFactory(
  minOpenConnections: Int,
  maxOpenConnections: Int,
  checkConnectionHealthWhenIdleFor: Duration,
  maxWaitForConnectionReservation: Duration,
  checkConnectionHealthOnReservation: Boolean,
  evictConnectionIfIdleFor: Duration,
  defaultUrlOptions: Map[String, String]) extends DatabaseFactory {

  def this(minConns: Int, maxConns: Int, checkIdle: Duration, maxWait: Duration, checkHealth: Boolean, evictTime: Duration) =
    this(minConns, maxConns, checkIdle, maxWait, checkHealth, evictTime, Map.empty)

  def apply(
    dbhosts: Seq[String],
    dbname: String,
    username: String,
    password: String,
    urlOptions: Map[String, String],
    driver: DatabaseDriver): ApachePoolingDatabase = {
    val finalUrlOptions =
      if (urlOptions eq null) {
        defaultUrlOptions
      } else {
        defaultUrlOptions ++ urlOptions
      }

    new ApachePoolingDatabase(
      dbhosts,
      dbname,
      username,
      password,
      finalUrlOptions,
      driver,
      minOpenConnections,
      maxOpenConnections,
      checkConnectionHealthWhenIdleFor,
      maxWaitForConnectionReservation,
      checkConnectionHealthOnReservation,
      evictConnectionIfIdleFor
    )
  }
}

class ApachePoolingDatabase(
  val hosts: Seq[String],
  val name: String,
  val username: String,
  password: String,
  val extraUrlOptions: Map[String, String],
  val driver: DatabaseDriver,
  minOpenConnections: Int,
  maxOpenConnections: Int,
  checkConnectionHealthWhenIdleFor: Duration,
  val openTimeout: Duration,
  checkConnectionHealthOnReservation: Boolean,
  evictConnectionIfIdleFor: Duration)
  extends Database {

  driver.load()

  private val connectionFactory = new DriverManagerConnectionFactory(url(hosts, name, urlOptions), username, password)
  private val poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null)
  poolableConnectionFactory.setValidationQuery("/* ping */ SELECT 1")
  poolableConnectionFactory.setDefaultReadOnly(false)
  poolableConnectionFactory.setDefaultAutoCommit(true)
  private val connectionPool = {
    val config = new GenericObjectPoolConfig
    config.setMaxTotal(maxOpenConnections)
    config.setMaxIdle(maxOpenConnections)
    config.setMinIdle(minOpenConnections)
    config.setMaxWaitMillis(openTimeout.inMilliseconds)
    config.setTestOnBorrow(checkConnectionHealthOnReservation)
    config.setMinEvictableIdleTimeMillis(evictConnectionIfIdleFor.inMilliseconds)
    config.setTimeBetweenEvictionRunsMillis(checkConnectionHealthWhenIdleFor.inMilliseconds)
    config.setTestWhileIdle(false)
    config.setLifo(false)
    new GenericObjectPool[PoolableConnection](poolableConnectionFactory, config)
  }
  poolableConnectionFactory.setPool(connectionPool)
  private val poolingDataSource = new PoolingDataSource(connectionPool)
  poolingDataSource.setAccessToUnderlyingConnectionAllowed(true)

  override def close(connection: Connection): Unit = {
    try {
      connection.close()
    } catch {
      case _: SQLException =>
    }
  }

  override def shutdown(): Unit = connectionPool.close()

  override def open(): Connection = poolingDataSource.getConnection

  override def toString: String = hosts.head + "_" + name
}
