package com.twitter.querulous

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

class DaemonThreadFactory(nameSuffix: String) extends ThreadFactory {
  private val group = new ThreadGroup(Thread.currentThread().getThreadGroup, "querulous-" + nameSuffix)
  private val threadNumber = new AtomicInteger(1)

  override def newThread(r: Runnable): Thread = {
    val thread = new Thread(group, r, "querulous-%s-%d".format(nameSuffix, threadNumber.getAndIncrement()))
    if (!thread.isDaemon) {
      thread.setDaemon(true)
    }
    if (thread.getPriority != Thread.NORM_PRIORITY) {
      thread.setPriority(Thread.NORM_PRIORITY)
    }
    thread
  }
}
