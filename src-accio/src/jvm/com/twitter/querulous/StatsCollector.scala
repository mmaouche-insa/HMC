package com.twitter.querulous

import scala.compat.Platform
import scala.concurrent.{ExecutionContext, Future}

trait StatsCollector {
  def incr(name: String, count: Int): Unit

  def time[A](name: String)(f: => A): A

  def addGauge(name: String)(gauge: => Double): Unit = {}

  def addMetric(name: String, value: Int): Unit = {}

  def timeFutureMillis[T](name: String)(f: Future[T])(implicit ctx: ExecutionContext): Unit = {
    val start = Platform.currentTime
    f.onComplete { _ =>
      addMetric(name + "_msec", (Platform.currentTime - start).toInt)
    }
  }
}

object NullStatsCollector extends StatsCollector {
  override def incr(name: String, count: Int): Unit = {}

  override def time[A](name: String)(f: => A): A = f

  override def timeFutureMillis[T](name: String)(f: Future[T])(implicit ctx: ExecutionContext) = {}
}