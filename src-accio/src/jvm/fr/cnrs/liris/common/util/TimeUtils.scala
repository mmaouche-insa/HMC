/*
 * Accio is a program whose purpose is to study location privacy.
 * Copyright (C) 2016-2017 Vincent Primault <vincent.primault@liris.cnrs.fr>
 *
 * Accio is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Accio is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Accio.  If not, see <http://www.gnu.org/licenses/>.
 */

package fr.cnrs.liris.common.util

import org.joda.time.Duration

/**
 * Helpers dealing with time and duration.
 */
object TimeUtils {
  /**
   * Convert a duration to a user-friendly string representation.
   *
   * @param time Duration.
   */
  def prettyTime(time: Duration): String = {
    val ms = time.getMillis.toDouble
    if (ms < 10.0) {
      f"$ms%.2f ms"
    } else if (ms < 100.0) {
      f"$ms%.1f ms"
    } else if (ms < 1000.0) {
      f"$ms%.0f ms"
    } else {
      f"${ms / 1000}%.3f s"
    }
  }

  /**
    * Prints the execution time of a portion of code
    *
    *
    * @param block Portion of Code.
    */

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  /**
    * Prints the execution time of a portion of code with a message
    *
    * @param message
    * @param block Portion of Code.
    */

  def time[R](message : String , block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    val dt = (t1 - t0) / 1000000
    println(s"$message : $dt ms")
    result
  }

}
