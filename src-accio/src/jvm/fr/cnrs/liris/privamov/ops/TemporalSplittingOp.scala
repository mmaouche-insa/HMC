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

import org.joda.time.DateTimeConstants

import scala.collection._
import scala.util.Random


@Op(
  category = "prepare",
  help = "Splits the traces based on the starting and ending time")
class TemporalSplittingOp  extends Operator[TemporalSplittingIn, TemporalSplittingOut] with SparkleOperator {


  override def execute(in: TemporalSplittingIn, ctx: OpContext): TemporalSplittingOut = {
    // read the data
    val ds = read[Trace](in.data)
    val output = split(ds,in.divider,in.complement)
    val nbEvents = ds.map(trace => trace.user -> trace.events.length)
    val nbEventsNew = output.map(trace => trace.user -> trace.events.length)
    val minOld = nbEvents.map( k => k._2).toArray.min
    val minNew = nbEventsNew.map( k => k._2).toArray.min
    val maxOld = nbEvents.map( k => k._2).toArray.max
    val maxNew = nbEventsNew.map( k => k._2).toArray.max
    val nbOld = nbEvents.map( k => k._2).toArray.sum
    val nbNew = nbEventsNew.map( k => k._2).toArray.sum
    val avgOld = nbOld.toDouble / nbEvents.keys.length.toDouble
    val avgNew = nbNew.toDouble / nbEventsNew.keys.length.toDouble
    val stdOld =  math.sqrt(nbEvents.map(k => math.pow(k._2.toDouble - avgOld,2)).toArray.sum / nbEvents.keys.length.toDouble)
    val stdNew =  math.sqrt(nbEventsNew.map(k => math.pow(k._2.toDouble - avgNew,2)).toArray.sum / nbEventsNew.keys.length.toDouble)

    TemporalSplittingOut(write(output, ctx.workDir),minOld,minNew,maxOld,maxNew,nbOld,nbNew,avgOld,avgNew,stdOld,stdNew)
  }

  def split(ds: DataFrame[Trace], divider: Double, complement :  Boolean) : DataFrame[Trace] = {
    ds.map{
      t =>
        if( t.events.nonEmpty) {
          val precision = 1000
          val multiple = math.ceil((1/divider) * precision).toInt
          val start_trace = t.events.head.time
          val durationTrain = t.duration.minus(t.duration.multipliedBy(multiple).dividedBy(precision))
          val timeTestStart = start_trace.plus(durationTrain)
          t.filter {
            e =>
              val b = e.time.isBefore(timeTestStart)
              if (!complement) b else !b
          }
        }else t
    }
  }
}

case class TemporalSplittingIn(
                       @Arg(help = "Input dataset")
                       data: Dataset,
                       @Arg(help = "Proportion of the test set in the form 1/divider ")
                       divider: Double,
                        @Arg(help = " Train : false , test : true ")
                         complement : Boolean
)

case class TemporalSplittingOut(
                        @Arg(help = "Output dataset")
                        output: Dataset,
                        @Arg(help = "Number of events of old dataset") minOld: Double,
                        @Arg(help = "Number of events of new dataset") minNew: Double,
                        @Arg(help = "Number of events of old dataset") maxOld: Double,
                        @Arg(help = "Number of events of new dataset") maxNew: Double,
                        @Arg(help = "Number of events of old dataset") sumOld: Double,
                        @Arg(help = "Number of events of new dataset") sumNew: Double,
                        @Arg(help = "Average number of events in old dataset") avgOld: Double,
                        @Arg(help = "Average number of events in new dataset") avgNew: Double,
                        @Arg(help = "Standard deviation of number of events by user in old dataset") stdOld: Double,
                        @Arg(help = "Standard deviation of number of events by user in old dataset") stdNew: Double)
