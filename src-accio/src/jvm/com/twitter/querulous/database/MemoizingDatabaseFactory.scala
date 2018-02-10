package com.twitter.querulous.database

import com.twitter.querulous.driver.DatabaseDriver

import scala.collection.mutable

class MemoizingDatabaseFactory(databaseFactory: DatabaseFactory) extends DatabaseFactory {
  // TODO: Use CacheBuilder after upgrading the Guava dependency to >= v10.
  private val databases = new mutable.HashMap[String, Database]

  def apply(dbhosts: Seq[String], dbname: String, username: String, password: String, urlOptions: Map[String, String], driver: DatabaseDriver) = synchronized {
    databases.getOrElseUpdate(
      dbhosts.toString + "/" + dbname,
      databaseFactory(dbhosts, dbname, username, password, urlOptions, driver))
  }

  // cannot memoize a connection without specifying a database
  override def apply(dbhosts: Seq[String], username: String, password: String) = databaseFactory(dbhosts, username, password)
}
