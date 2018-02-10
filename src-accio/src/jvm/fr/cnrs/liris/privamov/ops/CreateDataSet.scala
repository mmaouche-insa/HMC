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

import scala.collection.{mutable, _}
import scala.util.Random


@Op(
  category = "Prepare",
  help = "Create a new dataset with filtering")
class CreateDataSetOp extends Operator[CreateDataSetIn, CreateDataSetOut] with SparkleOperator {




  override def execute(in: CreateDataSetIn, ctx: OpContext): CreateDataSetOut = {
    // read the data
   val stats =  in.typeFilter match {
      case "full10" =>    full10day(in,ctx)

    }
    CreateDataSetOut(stats)
  }


def addDay(filename : String , dayTrace : Seq[Event] ) : Unit = {
  val bw = new BufferedWriter(new FileWriter(new File(filename)))

  dayTrace.foreach {
    dayE =>
      val str = s"${dayE.point.toLatLng.lat.degrees()},${dayE.point.toLatLng.lng.degrees()},${dayE.time.toInstant.getMillis}\n"
      bw.write(str)
  }

  bw.close()
}

  def insertOrNot(tracesList : Vector[Seq[Event]], nbEvents : Int, limit : Int ): Boolean ={
    tracesList.size < limit || tracesList.last.size < nbEvents
  }

  def full10day(in: CreateDataSetIn, ctx: OpContext) : immutable.Map[String,Int] ={
    // read the data
    val ds = read(in.data, env)
val limit = in.limit
    var stats = immutable.Map[String, Int]()
    ds.foreach {
      t =>
        var tracesList = Vector[Seq[Event]]() // list of all taken days
        val user = t.user
        val events = t.events // all the events


        var dayTrace: Seq[Event] = Seq[Event]() // current days list of events
        var day = events.head.time.toDateTime.dayOfYear() // day of start
        for (i <- events.indices) {
          val e = events(i)
          val newDay = e.time.toDateTime.dayOfYear() // current day
          if (newDay == day) {
            dayTrace = dayTrace :+ e
          }else {
           // save the day trace
            if ( (tracesList.size <= limit || tracesList.head.size < dayTrace.size) && dayTrace.nonEmpty){
              tracesList = tracesList :+ dayTrace
              tracesList = tracesList.sortBy(_.size)
              if(tracesList.size > limit) tracesList = tracesList.slice(1,limit+1)
            }
            day = newDay
            dayTrace = Seq[Event](e)
          }
         if(i == events.indices.last){
           // save the day trace
           if ( (tracesList.size <= limit || tracesList.head.size < dayTrace.size) && dayTrace.nonEmpty){
             tracesList = tracesList :+ dayTrace
             tracesList = tracesList.sortBy(_.size)
             if(tracesList.size > limit) tracesList = tracesList.slice(1,limit+1)
           }
         }
        }
        val filename = s"${ctx.workDir.getParent.getParent}/$user.csv"
       // println(filename)

        tracesList = tracesList.sortBy(_.head.time.toDateTime.getDayOfYear)
        for(k <- tracesList.indices){
           val l = tracesList(k)
         println(s"${t.user}_$k -> ${l.size}")
            stats += s"${user}_$k" -> l.size
            addDay(filename,l)
         }
        }
stats
    }
}

case class CreateDataSetIn(
                                 @Arg(help = "Input dataset")
                                 data: Dataset,
                                 @Arg(help = "Number of days")
                                 limit : Int = 10,
                                  @Arg(help = "Filter type")
                                  typeFilter : String = "full10"
                               )

case class CreateDataSetOut(
                                @Arg(help = "Statistics of days for each user")
                            stats : immutable.Map[String, Int]
                   )



