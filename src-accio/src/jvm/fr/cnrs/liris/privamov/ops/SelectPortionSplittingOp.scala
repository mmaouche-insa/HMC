
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
import java.io._

import fr.cnrs.liris.common.util.TimeUtils
import org.joda.time.Duration
import org.joda.time.DateTimeConstants

import scala.collection._
import scala.util.Random


@Op(
  category = "prepare",
  help = "Splits the traces based on the starting and ending time")
class SelectPortionSplittingOp  extends Operator[SelectPortionSplittingIn, SelectPortionSplittingOut] with SparkleOperator {


  override def execute(in: SelectPortionSplittingIn, ctx: OpContext): SelectPortionSplittingOut = {
   require(in.start < in.end && in.start >= 0 && in.end <= 1 )
    // read the data

    val ds = read[Trace](in.data)
    val output = ds.map(t => split(t,in.start,in.end))

    SelectPortionSplittingOut(write[Trace](output, ctx))
  }

  def split(t : Trace, start : Double, end : Double) = {
        if( t.events.nonEmpty) {
          val precision = 1000
          //val d = DateTimeConstants.MILLIS_PER_DAY*30
          val startMul =  math.floor(start*precision).toInt
          val endMul =  math.floor(end*precision).toInt
          val start_trace = t.events.head.time
          val duration = t.duration
        // val duration = new Duration(d)
          val timeStart =  start_trace.plus(duration.multipliedBy(startMul).dividedBy(precision))
          val timeEnd =  start_trace.plus(duration.multipliedBy(endMul).dividedBy(precision))
          t.filter(e => e.time.isAfter(timeStart) && e.time.isBefore(timeEnd))
        }else t
    }
}

case class SelectPortionSplittingIn(
                                @Arg(help = "Input dataset")
                                data: Dataset,
                                @Arg(help = " start portion")
                                start: Double,
                                @Arg(help = " End portion")
                                end: Double
                              )

case class SelectPortionSplittingOut(
                                 @Arg(help = "Output dataset")
                                 output: Dataset

                               )

