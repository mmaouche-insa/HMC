package com.twitter.querulous

import java.sql.Connection

import org.apache.commons.dbcp2.DelegatingConnection

trait DestroyableConnection {
  def destroy(): Unit
}

// Emergency connection destruction toolkit
trait ConnectionDestroying {
  def destroyConnection(conn: Connection): Unit = {
    if (!conn.isClosed) {
      conn match {
        case c: DelegatingConnection[_] =>
          destroyDbcpWrappedConnection(c)
        //case c: MySQLConnection =>
        //  c.abortInternal()
        case c: DestroyableConnection =>
          c.destroy()
        case c =>
          sys.error(s"Unsupported driver type, cannot reliably timeout: ${c.getClass.getName}")
      }
    }
  }

  def destroyDbcpWrappedConnection(conn: DelegatingConnection[_]): Unit = {
    val inner = conn.getInnermostDelegate
    if (inner ne null) {
      destroyConnection(inner)
    } else {
      // might just be a race; move on.
      return
    }
    // "close" the wrapper so that it updates its internal bookkeeping, just do it
    try {
      conn.close()
    } catch {
      case _: Throwable =>
    }
  }
}
