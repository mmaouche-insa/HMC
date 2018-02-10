/*
 * Copyright LIRIS-CNRS (2017)
 * Contributors: Mohamed Maouche  <mohamed.maouchet@liris.cnrs.fr>
 *
 * This software is a computer program whose purpose is to study location privacy.
 *
 * This software is governed by the CeCILL-B license under French law and
 * abiding by the rules of distribution of free software. You can use,
 * modify and/ or redistribute the software under the terms of the CeCILL-B
 * license as circulated by CEA, CNRS and INRIA at the following URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty and the software's author, the holder of the
 * economic rights, and the successive licensors have only limited liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading, using, modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean that it is complicated to manipulate, and that also
 * therefore means that it is reserved for developers and experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systems and/or
 * data to be ensured and, more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL-B license and that you accept its terms.
 */

package fr.cnrs.liris.privamov.ops

import com.google.inject.Inject
import fr.cnrs.liris.accio.core.api._
import fr.cnrs.liris.common.geo.{Distance, LatLng, Point}
import fr.cnrs.liris.privamov.core.model.Trace
import fr.cnrs.liris.privamov.core.sparkle.{DataFrame, SparkleEnv}
import org.joda.time.Duration

@Op(
  category = "prepare",
  help = "Enforce a given size on each trace.",
  description = "Larger traces will be truncated, smaller traces will be discarded.")
class CountPassThroughOp  extends Operator[CountPassThroughIn, CountPassThroughOut] with SparkleOperator {

  override def execute(in: CountPassThroughIn, ctx: OpContext): CountPassThroughOut = {
    val dstrain = read(in.train, env)
    val dstest = read(in.test, env)
    val loc = LatLng.degrees(in.lat, in.lng).toPoint
    val refCounts = computeCounts(dstrain,loc,in.minDistance)
    val resCounts = computeCounts(dstest,loc,in.minDistance)
    val results = (refCounts.keySet ++ resCounts.keySet).map(k => k -> compute(refCounts.getOrElse(k,default = 0),resCounts.getOrElse(k,0) )).toMap
   val avg = results.values.sum/results.keySet.size
    CountPassThroughOut(refCounts,resCounts,results,avg)
  }

  private def compute(refCount: Int, resCount: Int): Double = {
    if (0 == refCount) resCount
    else Math.abs(refCount - resCount).toDouble / refCount
  }

  private def computeCounts(data : DataFrame[Trace],loc : Point, minDistance : Distance) = {
    var mapCounts = Map[String,Int]()
    data.foreach{
      t =>
        val countsvalue = countVisit(t,loc,minDistance)
        synchronized(mapCounts += t.id -> countsvalue)
    }
    mapCounts
  }
  private def counts(t : Trace, loc : Point, minDistance : Distance) = {
    t.events.count(e => e.point.distance(loc) <= minDistance)
  }

  private def countVisit(t : Trace, loc : Point, minDistance : Distance) = {
    var c = 0
    var inside = false
    for(e <- t.events){
      if(!inside && e.point.distance(loc) <= minDistance){
         inside = true
        c += 1
      } else if(inside && e.point.distance(loc) > minDistance){
        inside = false
      }
    }
    c
  }
  override def isUnstable(in: CountPassThroughIn): Boolean = true

}

case class CountPassThroughIn(
                               @Arg(help = "Location's Latitude") lat: Double,
                               @Arg(help = "Location's Longitude") lng: Double,
                               @Arg(help = "Location's Longitude") minDistance: Distance = Distance.meters(100),
                               @Arg(help = "Train dataset") train: Dataset,
                               @Arg(help = "Test dataset") test: Dataset)

case class CountPassThroughOut(
                                @Arg(help = "Train Counts") refCounts: Map[String,Int],
                                @Arg(help = "Test Counts") resCounts: Map[String,Int],
                                @Arg(help = "Distortion Counts") distortions: Map[String,Double],
                                @Arg(help = "Average distortion") avgDistortion: Double
                              )