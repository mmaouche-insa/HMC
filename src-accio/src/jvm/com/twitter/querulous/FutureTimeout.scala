package com.twitter.querulous

import java.util.concurrent.{TimeoutException => JTimeoutException, _}

import com.twitter.util.Duration

class FutureTimeout(poolSize: Int, queueSize: Int) {
  private val executor = new ThreadPoolExecutor(
    1, /* corePoolSize */
    poolSize, /* maximumPoolSize */
    60, /* keepAliveTime */
    TimeUnit.SECONDS,
    new LinkedBlockingQueue[Runnable](queueSize),
    new DaemonThreadFactory("futureTimeout")
  )

  class Task[T](f: => T)(onTimeout: T => Unit) extends Callable[T] {
    private var cancelled = false
    private var result: Option[T] = None

    override def call: T = {
      val rv = Some(f)
      synchronized {
        result = rv
        if (cancelled) {
          result.foreach(onTimeout(_))
          throw new TimeoutException
        } else {
          result.get
        }
      }
    }

    def cancel(): Unit = synchronized {
      cancelled = true
      result.foreach(onTimeout(_))
    }
  }

  def apply[T](timeout: Duration)(f: => T)(onTimeout: T => Unit): T = {
    val task = new Task(f)(onTimeout)
    val future = new FutureTask(task)
    try {
      executor.execute(future)
      future.get(timeout.inMilliseconds, TimeUnit.MILLISECONDS)
    } catch {
      case e: JTimeoutException =>
        task.cancel()
        throw new TimeoutException
      case e: RejectedExecutionException =>
        task.cancel()
        throw new TimeoutException
    }
  }
}
