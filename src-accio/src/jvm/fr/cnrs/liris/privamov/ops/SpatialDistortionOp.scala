/*
 * Copyright LIRIS-CNRS (2016)
 * Contributors: Vincent Primault <vincent.primault@liris.cnrs.fr>
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
import fr.cnrs.liris.common.geo.{Distance, Point}
import fr.cnrs.liris.common.stats.AggregatedStats
import fr.cnrs.liris.common.util.Requirements._
import fr.cnrs.liris.privamov.core.model.Trace
import fr.cnrs.liris.privamov.core.sparkle.SparkleEnv

@Op(
  category = "metric",
  help = "Compute spatial distortion between two datasets of traces")
class SpatialDistortionOp  extends Operator[SpatialDistortionIn, SpatialDistortionOut] with SparkleOperator {

  override def execute(in: SpatialDistortionIn, ctx: OpContext): SpatialDistortionOut = {
    val train = read(in.train, env)
    val test = read(in.test, env)
    val metrics = train.zip(test).map {
      case (ref, res) =>
        //println(s"LUNCH OF :\t user = ${ref.user}\t length = ${ref.events.size}\t user = ${res.user}\t length = ${res.events.size}  ")
        val stats = evaluate(ref, res, in.interpolate)
        //println(s"FINISH OF :\t user = ${ref.user}")

        stats
    }.toArray.toMap


    println(s"\n\n Size = ${metrics.values.size}")

   val min = metrics.map { case (k, v) => k -> v.min }
   val max = metrics.map { case (k, v) => k -> v.max }
   val stddev = metrics.map { case (k, v) => k -> v.stddev }
   val avg = metrics.map { case (k, v) => k -> v.avg }
    val median = metrics.map { case (k, v) => k -> v.median }
    SpatialDistortionOut(
      min ,
      max ,
      stddev ,
      avg ,
      median ,
      dmin = min.values.sum / min.values.size,
      dmax = max.values.sum / max.values.size,
      dstddev =stddev.values.sum / stddev.values.size,
      davg = avg.values.sum / avg.values.size,
      dmedian = median.values.sum / median.values.size
    )
  }

  private def evaluate(ref: Trace, res: Trace, interpolate: Boolean) = {
    requireState(ref.id == res.id, s"Trace mismatch: ${ref.id} / ${res.id}")
    require(res.isEmpty || ref.size >= 1, s"Cannot evaluate spatial distortion with empty reference trace ${ref.id}")
    val points = ref.events.map(_.point)
    val distances = if (interpolate) {
      evaluateWithInterpolation(points, res)
    } else {
      evaluateWithoutInterpolation(points, res)
    }
    ref.id -> AggregatedStats(distances.map(_.meters))
  }

  private def evaluateWithoutInterpolation(reference: Seq[Point], result: Trace) =
    result.events.par.map(event => Point.nearest(event.point, reference).distance).seq

 /* private def evaluateWithInterpolation(reference: Seq[Point], result: Trace) = {
    result.events.map { event =>
      val (a, b) = nearestLine(event.point, reference)
      val projected = if (a == b) a else projectToLine(event.point, a, b)
      val d = event.point.distance(projected)
      println(s"distance = $d  idx = ${result.events.indexOf(event)}")
      d
    }
  }*/

  /*private def nearestLine(point: Point, line: Seq[Point]) = {
    val a = Point.nearest(point, line)
    val b = if (a.idx == 0) {
      line(1)
    } else if (a.idx == line.size - 1) {
      line(a.idx - 1)
    } else {
      //Point.nearest(point, Seq(line(a.idx - 1), line(a.idx + 1))).point
      val b1 = line(a.idx - 1)
        val b2 = line(a.idx + 1)
      val h1 = projectToLine(point,a.point,b1)
      val h2 = projectToLine(point,a.point,b2)
      println(s"h1 : ${point.distance(h1)} h2 : ${point.distance(h2)}")
      if(point.distance(h1)<point.distance(h2)) b1 else b2
    } //TODO: should check for previous/next non-identical point
    (a.point, b)
  }*/

   private def evaluateWithInterpolation(reference: Seq[Point], result: Trace) = {
   result.events.par.map { event =>
     val (a, b) = nearestLine(event.point, reference)
     val projected =  projectToLine(event.point, a, b)
     val d = event.point.distance(projected)
    // println(s"distance = $d  idx = ${result.events.indexOf(event)}  lat = ${projected.toLatLng.lat}  lng = ${projected.toLatLng.lng}")
     d
   }.seq
 }

  private def nearestLine(point: Point, line: Seq[Point]) = {
  var min : Option[Distance] = None
    var p1 = line.head
    var p2 = line.last
    val lineWithIndex = line.zipWithIndex
    lineWithIndex.foreach{
    case (pt,i) =>
      if(i != line.length -1) {
        val h = projectToLine(point,pt,lineWithIndex(i+1)._1)
        val d = point.distance(h)
        min match {
          case None => min = Some(d)
          case Some(dprime) => {
            if(d < dprime ) {
              min = Some(d)
              p1 = pt
              p2 = lineWithIndex(i+1)._1
            }
          }
        }
      }

  }
  (p1,p2)
}



  private def projectToLine(point: Point, a: Point, b: Point, snap: Boolean = true) = {
    // http://stackoverflow.com/questions/1459368/snap-point-to-a-line-java
    val apx = point.x - a.x
    val apy = point.y - a.y
    val abx = b.x - a.x
    val aby = b.y - a.y

    val ab2 = abx * abx + aby * aby
    val ap_ab = apx * abx + apy * aby
    var t = ap_ab / ab2
    if (snap) {
      if (t < 0) {
        t = 0
      } else if (t > 1) {
        t = 1
      }
    }
    a + Point(abx * t, aby * t)
  }
}

case class SpatialDistortionIn(
  @Arg(help = "Whether to interpolate between points") interpolate: Boolean = true,
  @Arg(help = "Train dataset") train: Dataset,
  @Arg(help = "Test dataset") test: Dataset)

case class SpatialDistortionOut(
  @Arg(help = "Spatial distortion min") min: Map[String, Double],
  @Arg(help = "Spatial distortion max") max: Map[String, Double],
  @Arg(help = "Spatial distortion stddev") stddev: Map[String, Double],
  @Arg(help = "Spatial distortion avg") avg: Map[String, Double],
  @Arg(help = "Spatial distortion median") median: Map[String, Double],

    @Arg(help = "Dataset Spatial distortion min") dmin: Double,
@Arg(help = "Dataset Spatial distortion max") dmax: Double,
@Arg(help = "Dataset Spatial distortion stddev") dstddev: Double,
@Arg(help = "Datase tSpatial distortion avg") davg: Double,
@Arg(help = "Dataset Spatial distortion median") dmedian: Double

)