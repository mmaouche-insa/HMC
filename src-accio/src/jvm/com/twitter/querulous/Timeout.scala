package com.twitter.querulous

import java.util.logging.{Level, Logger}
import java.util.{Timer, TimerTask}

import com.twitter.util.Duration

class TimeoutException extends Exception

object Timeout {
  private val defaultTimer = new Timer("Timer thread", true)

  def apply[T](timer: Timer, timeout: Duration)(f: => T)(onTimeout: => Unit): T = {
    @volatile var cancelled = false
    val task =
      if (timeout.inMilliseconds > 0) {
        Some(schedule(timer, timeout, {
          cancelled = true
          onTimeout
        }))
      } else {
        None
      }

    try {
      f
    } finally {
      task map { t =>
        t.cancel()
        // TODO(benjy): Timer is not optimized to deal with large numbers of cancellations: it releases and reacquires its monitor
        // on every task, cancelled or not, when it could quickly skip over all cancelled tasks in a single monitor region.
        // This may not be a problem, but it's something to be aware of.
      }
      if (cancelled) throw new TimeoutException
    }
  }

  def apply[T](timeout: Duration)(f: => T)(onTimeout: => Unit): T = {
    apply(defaultTimer, timeout)(f)(onTimeout)
  }

  private def schedule(timer: Timer, timeout: Duration, f: => Unit) = {
    val task = new TimerTask() {
      override def run(): Unit = {
        try {
          f
        } catch {
          case e: Throwable =>
            val l = Logger.getLogger("querulous")
            l.log(Level.WARNING, "Timeout task tried to throw an exception", e)
        }
      }
    }
    timer.schedule(task, timeout.inMilliseconds)
    task
  }
}
