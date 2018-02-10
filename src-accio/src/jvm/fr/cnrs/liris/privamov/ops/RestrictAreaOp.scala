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

import com.google.inject.Inject
import fr.cnrs.liris.accio.core.api._
import fr.cnrs.liris.common.geo._

import fr.cnrs.liris.privamov.core.model.Trace
import fr.cnrs.liris.privamov.core.sparkle.{DataFrame, SparkleEnv}

import com.google.common.geometry._

@Op(
  category = "Prepare",
  help = "Restrict a dataset on a almost rectangular area")
class RestrictAreaOp  extends Operator[RestrictAreaIn, RestrictAreaOut] with SparkleOperator {

  override def execute(in: RestrictAreaIn, ctx: OpContext): RestrictAreaOut = {

    // read the data
    val ds = read(in.data, env)
    // Prepare the restrictive box
    var p1 = Point(0,0)
    var p2 = Point(0,0)
    ( in.lat2 -> in.lng2 ) match {
   case (Some(lt) , Some(lg)) => {
     p1 = LatLng.degrees(in.lat1, in.lng1).toPoint
     p2 = LatLng.degrees(lt, lg).toPoint
   }
   case _ => {
    p1 = LatLng.degrees(in.lat1, in.lng1).toPoint

    p2 = p1.translate(S1Angle.degrees(in.ang), in.diag)
    }
 }
    val bounder = BoundingBox(p1, p2)
    // Restrict the tracers to the region.
    val output: DataFrame[Trace]= ds.map { t =>
      val newt = t.filter { e =>  bounder.contains(e.point)}
      newt
    }
    var nbTot = 0L
    var nbTaken = 0L
    ds.foreach(t => nbTot += t.events.size)
    output.foreach(t => nbTaken += t.events.size)
    val ratio = nbTaken.toDouble / nbTot.toDouble

    // println(s"ratio taken =  $ratio")
    RestrictAreaOut(write(output, ctx.workDir), ratio)
  }
}


case class RestrictAreaIn(


                           @Arg(help = "Input dataset")
                           data: Dataset,
                           @Arg(help = "Lower point latitude")
                           lat1: Double,
                           @Arg(help = "Lower point longitude")
                           lng1: Double,
                           @Arg(help = "Diagonal distance")
                           diag: Distance,
                           @Arg(help = "Diagona angle (degrees)")
                           ang: Double = 45.0,
                           @Arg (help = "Higher point latitude (override diag & angle)")
                          lat2: Option[Double],
                          @Arg (help = "Higher point latitude (override diag & angle)")
                           lng2: Option[Double] )




case class RestrictAreaOut(
                            @Arg(help = "Output dataset") data: Dataset,
                            @Arg(help = "Ratio of taken points") ratio: Double

                          )