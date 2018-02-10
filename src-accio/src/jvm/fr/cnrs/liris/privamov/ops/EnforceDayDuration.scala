/*
 * Copyright LIRIS-CNRS (2017)
 * Contributors: Mohamed Maouche  <mohamed.maouchet@liris.cnrs.fr>
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

// @TODO NEED TESTING
package fr.cnrs.liris.privamov.ops

import com.google.inject.Inject
import fr.cnrs.liris.accio.core.api._
import fr.cnrs.liris.common.geo._
import fr.cnrs.liris.privamov.core.model.{Event, Trace}
import fr.cnrs.liris.privamov.core.sparkle.{DataFrame, SparkleEnv}
import com.google.common.geometry._
import java.io._

import org.joda.time.DateTimeConstants

import scala.collection._
import scala.util.Random


@Op(
  category = "Prepare",
  help = "Re-identification attack based on the matrix matching")
class EnforceDayDurationOp  extends Operator[EnforceDayDurationIn, EnforceDayDurationOut] with SparkleOperator {


  override def execute(in: EnforceDayDurationIn, ctx: OpContext): EnforceDayDurationOut = {
    // read the data
    val ds = read(in.data, env)
    

    var nbPoints = 0
    ds.foreach(t => nbPoints += t.events.size)
    val minPoints = (nbPoints.toDouble*0.5/ds.keys.size.toDouble).toInt
  //  var stats: immutable.Map[String, (Int,Int)] = immutable.Map[String,(Int,Int)]()
    var statsTot: immutable.Map[String, Int] = immutable.Map[String, Int]()
    var statsTaken: immutable.Map[String, Int] = immutable.Map[String, Int]()

    ds.foreach {
      t =>
        var nbDaysTot = 0
        var nbDaysTaken = 0
        val user = t.user
       // val bw = new BufferedWriter(new FileWriter(new File(s"${ctx.workDir}/$user.csv")))
       val bw = new BufferedWriter(new FileWriter(new File(s"${ctx.workDir.getParent.getParent}/$user.csv")))
        val events = t.events
        var dayTrace: Seq[Event] = Seq[Event]()
        var day = events.head.time.toDateTime.dayOfYear()
        for (i <- events.indices) {
          val e = events(i)
          val newDay = e.time.toDateTime.dayOfYear()
          if (newDay != day) {
            nbDaysTot += 1
            day = newDay
            if (dayTrace.size >= minPoints) {
              nbDaysTaken += 1
              dayTrace.foreach {
                dayE =>
                  val str = s"${dayE.point.toLatLng.lat.degrees()},${dayE.point.toLatLng.lng.degrees()},${dayE.time.toInstant.getMillis}\n"
                  bw.write(str)
              }
              dayTrace = Seq[Event](e)
            }
          } else {
            dayTrace = dayTrace :+ e
          }
          if (i == events.indices.last) {
            if(dayTrace.nonEmpty) {
              nbDaysTot += 1
              if (dayTrace.size >= minPoints) {
                nbDaysTaken += 1
                dayTrace.foreach {
                  dayE =>
                    val str = s"${dayE.point.toLatLng.lat.degrees()},${dayE.point.toLatLng.lng.degrees()},${dayE.time.toInstant.getMillis}\n"
                    bw.write(str)
                }
              }
            }
          }
        }
        bw.close()
        //stats += (user -> (nbDaysTot, nbDaysTaken))
       statsTot += user -> nbDaysTot
        statsTaken += user -> nbDaysTaken

    }
    EnforceDayDurationOut(statsTot,statsTaken)
 //   EnforceDayDurationOut(stats)
  }
}

  case class EnforceDayDurationIn(
                                   @Arg(help = "Input dataset")
                                   data: Dataset,
                                   @Arg(help = "Input dataset")
                                   minPoints: Int = 1

                                 )

  case class EnforceDayDurationOut(
                                   @Arg(help = "Statistics of days for each user") statsTot: immutable.Map[String, Int],
                                    @Arg(help = "Statistics of days taken for each user") statsTaken: immutable.Map[String, Int]

                               //   @Arg(help = "Statistics of days taken for each user") stats: immutable.Map[String, (Int,Int)]
)


