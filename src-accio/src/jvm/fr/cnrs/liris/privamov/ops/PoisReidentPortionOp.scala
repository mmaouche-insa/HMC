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

package fr.cnrs.liris.privamov.ops

import java.util.Objects

import breeze.stats.DescriptiveStats
import com.github.nscala_time.time.Imports._
import com.google.common.base.MoreObjects
import com.google.inject.Inject
import fr.cnrs.liris.accio.core.api._
import fr.cnrs.liris.common.geo.{Distance, Point}
import fr.cnrs.liris.privamov.core.clustering.DTClusterer
import fr.cnrs.liris.privamov.core.model.{Event, Trace}
import fr.cnrs.liris.privamov.core.sparkle.{DataFrame, SparkleEnv}
import fr.cnrs.liris.common.util.StringUtils
import org.joda.time.Instant

/**
  * Implementation of a re-identification attack using the points of interest as a discriminating information. The POIs
  * are used to model the behaviour of training ids, and then extracted from the tracks of test ids and compared to
  * those from the training ids. The comparison here is done between set of POIs, and only the spatial information.
  *
  * Vincent Primault, Sonia Ben Mokhtar, Cédric Lauradoux and Lionel Brunie. Differentially Private
  * Location Privacy in Practice. In MOST'14.
  */
@Op(
  category = "metric",
  help = "Re-identification attack using POIs a the discriminating information.")
class PoisReidentPortionOp  extends Operator[ReidentificationPortionIn, ReidentificationPortionOut] with SparkleOperator {

  override def execute(in: ReidentificationPortionIn, ctx: OpContext): ReidentificationPortionOut = {
    val trainDs = read(in.train, env)
    val testDs = read(in.test, env)
   
    val trainClusterer = new DTClusterer(in.duration, in.diameter)
    val testClusterer = new DTClusterer(in.testDuration.getOrElse(in.duration), in.testDiameter.getOrElse(in.diameter))
   
    val trainPois = getPois(trainDs, trainClusterer)
    val testPois = getPois(testDs, testClusterer)

    val distances = getDistances(trainPois, testPois)
    var matches = getMatches(distances,trainDs.keys.intersect(testDs.keys).toSet)

   val successRate = getSuccessRateComplete(testDs.keys.length, matches)

        trainDs.keys.union(testDs.keys).foreach(s => if(!matches.contains(s)) matches +=  s -> "-")
    //ReidentificationPortionOut(distances.map { case (k, v) => k -> v.toMap }, matches, successRate)
    print("POI-Attack id re-identified : ")
    println(matches.filter{ pair => pair._1==pair._2}.keySet)
    println(s"POI-Attack rate : $successRate")
    ReidentificationPortionOut(matches, successRate)

  }

  private def getPois(data: DataFrame[Trace], clusterer: DTClusterer) = {
    val allPois = data.flatMap{t => clusterer.cluster(t).map(c => PoiPortion(c.events,t.id))}.toArray
    allPois.groupBy(_.id).map { case (id, pois) => PoiSetPortion(id, pois) }.toSeq
  }

  private def getDistances(trainPois: Seq[PoiSetPortion], testPois: Seq[PoiSetPortion]) = {
    val costs = collection.mutable.Map[String, Map[String, Double]]()
    testPois.foreach { pois =>
      val distances = if (pois.nonEmpty) {
        //Compute the distance between the set of pois from the test id and the models (from the training ids).
        //This will give us an association between a training id and a distance. We only keep finite distances.
        trainPois.map(model => model.id -> model.distance(pois).meters).filter { case (u, d) => !d.isInfinite }.toMap
      } else {
        Map[String, Double]()
      }
      costs.synchronized {
        costs += pois.id -> distances
      }
    }
    costs.toSeq.map { case (id, model) =>
      id -> model.toSeq.sortBy(_._2).map { case (u: String, d: Double) => (u, d) }
    }.toMap
  }

  private def getMatches(distances: Map[String, Seq[(String, Double)]], idFull : Set[String]) = {
   var matches = distances.map { case (testid, res) =>
      if (res.isEmpty) {
        testid -> "-"
      } else {
        testid -> res.head._1
      }
    }
    idFull.foreach { u =>
      if (!matches.contains(u)) matches += u -> "-"
    }
    matches
  }
  private def getSuccessRate(trainPois: Seq[PoiSetPortion], matches: Map[String, String]) = {
    val trainids = trainPois.map(_.id)
    val nbMataches = matches.map { case (testid, trainid) =>
      if (testid == trainid) {
        1
      }
      else {
        0
      }
    }.sum
    val nbids = trainids.size.toDouble
    nbMataches.toDouble / nbids.toDouble
  }

  private def getSuccessRateComplete(nbids : Int, matches: Map[String, String]) = {
    val nbMataches = matches.map { case (testid, trainid) =>
      if (StringUtils.compareUser(testid,trainid)) {
        1
      }
      else {
        0
      }
    }.sum
    nbMataches.toDouble / nbids.toDouble
  }
}
case class PoiPortion (
                id: String,
                centroid: Point,
                size: Int,
                start: Instant,
                end: Instant,
                diameter: Distance) {

  /**
    * Return the total amount of time spent inside this POI.
    */
  def duration: Duration = (start to end).duration

  /**
    * We consider two POIs are the same if they belong to the same id and are defined by the same centroid during
    * the same time window (we do not consider the other attributes).
    *
    * @param that Another object.
    * @return True if they represent the same POI, false otherwise.
    */
  override def equals(that: Any): Boolean = that match {
    case p: PoiPortion => p.id == id && p.centroid == centroid && p.start == start && p.end == end
    case _ => false
  }

  override def hashCode: Int = Objects.hash(id, centroid, start, end)
}

object PoiPortion{
  /**
    * Create a new PoiPortionfrom a list of events.
    *
    * @param events A non-empty list of events
    */
  def apply(events: Iterable[Event],id : String): PoiPortion= {
    val seq = events.toSeq.sortBy(_.time)
    require(seq.nonEmpty, "Cannot create a PoiPortionfrom an empty list of events")
    val centroid = Point.centroid(seq.map(_.point))
    val diameter = Point.fastDiameter(seq.map(_.point))
    new PoiPortion(id, centroid, seq.size, seq.head.time, seq.last.time, diameter)
  }

  /**
    * Create a PoiPortionfrom a single point and timestamp. Its duration will be zero and its diameter arbitrarily fixed
    * to 10 meters.
    *
    * @param id  id identifier
    * @param point Location of this POI
    * @param time  Timestamp
    */
  def apply(id: String, point: Point, time: Instant): PoiPortion=
    PoiPortion(id, point, 1, time, time, Distance.meters(10))

  /**
    * Create a PoiPortionfrom a single event. Its duration will be zero and its diameter arbitrarily fixed to 10 meters.
    *
    * @param event Event
    */
  def apply(event: Event,id : String): PoiPortion= apply(id, event.point, event.time)
}

/**
  * A set of POIs belonging to a single id. This is essentially a wrapper around a basic set, providing some
  * useful methods to manipulate POIs.
  *
  * @param id id identifier.
  * @param pois List of unique POIs.
  */
case class PoiSetPortion(id: String, pois: Seq[PoiPortion]) {
  /**
    * Check whether the set of POIs is not empty.
    *
    * @return True if there is at least one POI, false otherwise.
    */
  def nonEmpty: Boolean = pois.nonEmpty

  /**
    * Check whether the set of POIs is empty.
    *
    * @return True if there is no POIs, false otherwise.
    */
  def isEmpty: Boolean = pois.isEmpty

  /**
    * Return the number of POIs inside this set.
    */
  def size: Int = pois.size

  /**
    * Compute the minimum distance between POIs inside this set and another POI.
    *
    * @param poi POI to compute the distance with.
    */
  def distance(poi: PoiPortion): Distance = Point.nearest(poi.centroid, pois.map(_.centroid)).distance

  /**
    * Compute the distance with another set of POIs (it is symmetrical).
    *
    * @param that Another set of POIs to compute the distance with.
    */
  def distance(that: PoiSetPortion): Distance = distance(pois, that.pois)

  /**
    * Compute the distance between to sets of POIs (it is symmetrical).
    *
    * @param as First set of POIs.
    * @param bs Second set of POIs.
    */
  private def distance(as: Iterable[PoiPortion], bs: Iterable[PoiPortion]): Distance = {
    val a = as.map(_.centroid)
    val b = bs.map(_.centroid)
    val d = distances(a, b) ++ distances(b, a)
    if (d.nonEmpty) {
      Distance.meters(DescriptiveStats.percentile(d, 0.5))
    } else {
      Distance.Infinity
    }
  }

  /**
    * Compute all distances between each point in `a` and the closest point in `b` (it is *not* symmetrical).
    *
    * @param a A first set of points
    * @param b A second set of points
    */
  private def distances(a: Iterable[Point], b: Iterable[Point]): Iterable[Double] =
    if (b.isEmpty) {
      Iterable.empty[Double]
    } else {
      a.map(point => Point.nearest(point, b).distance.meters).filterNot(_.isInfinite)
    }

  override def toString: String =
    MoreObjects.toStringHelper(this)
      .add("id", id)
      .add("size", size)
      .toString
}

/**
  * Factory for [[PoiSetPortion]].
  */
object PoiSetPortion{
  /**
    * Create a new set of POIs.
    *
    * @param id id identifier.
    * @param pois List of POIs.
    */
  def apply(id: String, pois: Iterable[PoiPortion]): PoiSetPortion= new PoiSetPortion(id, pois.toSeq.distinct)
}
case class ReidentificationPortionIn(
                               @Arg(help = "Clustering maximum diameter")
                               diameter: Distance,
                               @Arg(help = "Clustering minimum duration")
                               duration: org.joda.time.Duration,
                               @Arg(help = "Override the clustering maximum diameter to use with the test dataset only")
                               testDiameter: Option[Distance],
                               @Arg(help = "Override the clustering minimum duration to use with the test dataset only")
                               testDuration: Option[org.joda.time.Duration],
                               @Arg(help = "Train dataset")
                               train: Dataset,
                               @Arg(help = "Test dataset")
                               test: Dataset)

case class ReidentificationPortionOut(
                               // @Arg(help = "Distances between ids from test and train datasets")
                                //distances: Map[String, Map[String, Double]],
                                @Arg(help = "Matches between ids from test and train datasets")
                                matches: Map[String, String],
                                @Arg(help = "Correct re-identifications rate")
                                rate: Double)