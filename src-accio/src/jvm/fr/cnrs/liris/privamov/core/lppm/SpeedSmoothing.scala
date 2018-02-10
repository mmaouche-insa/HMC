/*
 * Accio is a program whose purpose is to study location privacy.
 * Copyright (C) 2016 Vincent Primault <vincent.primault@liris.cnrs.fr>
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

package fr.cnrs.liris.privamov.core.lppm

import com.github.nscala_time.time.Imports._
import fr.cnrs.liris.common.geo.Distance
import fr.cnrs.liris.privamov.core.model.{Event, Trace}

import scala.collection.mutable

/**
 * Applies speed smoothing in order to remove points of interest from a mobility trace.
 *
 * Vincent Primault, Sonia Ben Mokhtar, Cédric Lauradoux, Lionel Brunie. Time Distortion
 * Anonymization for the Publication of Mobility Data with High Utility. In Proceedings of
 * TrustCom'15.
 *
 * @param epsilon Distance between two consecutive points.
 */
class SpeedSmoothing(epsilon: Distance) {


  def transform(trace: Trace): Trace =
    if (trace.isEmpty) {
      trace.empty
    } else if (epsilon == Distance.Zero) {
      trace
    } else {
      // We sample events to keep those at a distance of exactly `epsilon` from the previous one.
      // Sampled locations will be interpolated linearly between the two nearest reported
      // locations. This way there will be the same distance between two consecutive events.
      val sampled = sample(trace.events, epsilon)

      // The time to "spend" will be uniformely allocated. This way there will be the same
      // duration between two consecutive events.
      trace.replace(allocate(sampled))
    }


  def transform(events : Seq[Event]): Seq[Event] =
    if (events.isEmpty || (epsilon == Distance.Zero)) {
      events
    } else {
      // We sample events to keep those at a distance of exactly `epsilon` from the previous one.
      // Sampled locations will be interpolated linearly between the two nearest reported
      // locations. This way there will be the same distance between two consecutive events.
      val sampled = sample(events, epsilon)
      // The time to "spend" will be uniformely allocated. This way there will be the same
      // duration between two consecutive events.
      allocate(sampled)
    }


  private def sample(events: Seq[Event], epsilon: Distance) = {
    var sampled = mutable.ListBuffer.empty[Event]
    var prev: Option[Event] = None
    for (event <- events) {
      if (prev.isDefined) {
        var d = event.point.distance(prev.get.point)
        while (d >= epsilon) {
          // Generate as many points as needed to get from previous to current location by steps
          // of epsilon.
          val ratio = epsilon.meters / d.meters
          val newPoint = prev.get.point.interpolate(event.point, ratio)
          sampled += event.copy(point = newPoint)

          prev = Some(event.copy(point = newPoint))
          d = event.point.distance(prev.get.point)
        }
      } else {
        //First iteration, keep true location and time.
        sampled += event
        prev = Some(event)
      }
    }
    // We skip the potential first and last stay to maximize utility (there will likely be a stay
    // that will "consume" time budget) and sightly protect start and end points.
    if (sampled.nonEmpty) {
      sampled = sampled.drop(1)
    }
    if (sampled.nonEmpty) {
      sampled = sampled.dropRight(1)
    }
    sampled
  }

  private def allocate(sampled: Seq[Event]) =
    if (sampled.size <= 2) {
      sampled
    } else {
      val from = sampled.head.time
      val timeSpent = (from to sampled.last.time).millis
      val interval = timeSpent.toDouble / (sampled.size - 1)
      sampled.zipWithIndex.map { case (event, idx) =>
        val shift = math.ceil(idx * interval).toInt
        event.copy(time = from + shift)
      }
    }
}