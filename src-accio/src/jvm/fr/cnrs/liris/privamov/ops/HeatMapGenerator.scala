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
import java.nio.file.Files

import org.joda.time.DateTimeConstants

import scala.collection._
import scala.util.Random


@Op(
  category = "Attack",
  help = "Re-identification attack based on the matrix matching")
class HeatMapGeneratorOp extends Operator[HeatMapGeneratorIn, HeatMapGeneratorOut] with SparkleOperator {


  override def execute(in: HeatMapGeneratorIn, ctx: OpContext): HeatMapGeneratorOut = {
    // read the data
    val dstrain = read(in.data, env)
    // rectangular point
    val (p1, p2) = initializePoint(in)
    val (rdstrain, ratio) = restrictArea(dstrain, p1, p2)


    val dimensions = computeMatricesSize(p1, p2, in.cellSize)
   val maxCellCenter= if (in.heatmaps) {
      formHeatMaps(rdstrain, dimensions, in, ctx)

    } else immutable.Map[String,String]()
    val statstrain = datasetStats(dstrain)
    val ltrain = rdstrain.map(t => (t.user, t.events.size)).toArray.toMap

    HeatMapGeneratorOut(statstrain,ltrain ,maxCellCenter)
  }

  def formHeatMaps(rdstrain: DataFrame[Trace], dimensions: (Int, Int, Point), in: HeatMapGeneratorIn, ctx: OpContext) = {
    //  if(ctx.workDir.getParent.toFile.exists()) Files.createDirectories(ctx.workDir.toAbsolutePath)

    if (!ctx.workDir.toFile.exists()) Files.createDirectories(ctx.workDir.toAbsolutePath)

        val trainOutputMap = formSingleMatrices(rdstrain, dimensions, in.cellSize)
        printMatrixCsvfile(trainOutputMap, "train", ctx.workDir.toAbsolutePath.toString)

       trainOutputMap.map{f => f._1 -> computeCenterLatLng(f._2.data.max._1,dimensions,in).toString}


  }
def computeCenterLatLng(ind : (Int,Int),  dimensions: (Int, Int, Point) ,in: HeatMapGeneratorIn) = {
  val center = cellCenter(ind,dimensions._3,in.cellSize)
  center.toLatLng
}

  def cellCenter(ind: (Int, Int), up: Point, cellSize: Distance) = {
    val (p1x, p1y) = cellCornerLeftUp(ind, up, cellSize)
    val (p2x, p2y) = cellCornerRightDown(ind, up, cellSize)
    Point((p1x + p2x) / 2, (p1y + p2y) / 2)
  }
  def cellCornerLeftUp(ind: (Int, Int), up: Point, cellSize: Distance) = (up.x + ind._2 * cellSize.meters, up.y + ind._1 * cellSize.meters)

  def cellCornerLeftDown(ind: (Int, Int), up: Point, cellSize: Distance) = (up.x + ind._2 * cellSize.meters, up.y + (ind._1 + 1) * cellSize.meters)

  def cellCornerRightDown(ind: (Int, Int), up: Point, cellSize: Distance) = (up.x + (ind._2 + 1) * cellSize.meters, up.y + (ind._1 + 1) * cellSize.meters)

  def cellCornerRightUp(ind: (Int, Int), up: Point, cellSize: Distance) = (up.x + (ind._2 + 1) * cellSize.meters, up.y + ind._1 * cellSize.meters)



  def formSingleMatrices(ds: DataFrame[Trace], dimensions: (Int, Int, Point), cellSize: Distance): immutable.Map[String, MatrixLight[Int]] = {

    var outputMap: scala.collection.immutable.Map[String, MatrixLight[Int]] = scala.collection.immutable.Map[String, MatrixLight[Int]]().empty
    ds.foreach{ t =>
      synchronized(outputMap += (t.user -> new MatrixLight[Int](dimensions._1, dimensions._2)))
    }
    ds.foreach {
      t =>
        val l = t.events.length

        if (l != 0) {
          t.events.last.time
          val start_trace = t.events.head.time
          val user = t.user
          val events = t.events
          events.foreach { e =>
            val p = e.point
            val j = math.floor((p.x - dimensions._3.x) / cellSize.meters).toInt
            val i = math.floor((p.y - dimensions._3.y) / cellSize.meters).toInt
            val mat = outputMap(user)
            synchronized(mat.inc(i, j))
            synchronized(outputMap += (user -> mat))
          }
        }else{
          synchronized(outputMap  -= t.user)
        }
    }

    outputMap
  }

  def formDayHoursMatrices(ds: DataFrame[Trace], dimensions: (Int, Int, Point), cellSize: Distance): scala.collection.immutable.Map[String, Array[MatrixLight[Int]]] = {

    var outputMap: scala.collection.immutable.Map[String, Array[MatrixLight[Int]]] = scala.collection.immutable.Map[String, Array[MatrixLight[Int]]]().empty
    for (key <- ds.keys) {
      val tab = new Array[MatrixLight[Int]](24)
      for (i <- 0 to 23) tab(i) = new MatrixLight[Int](dimensions._1, dimensions._2)
      outputMap += (key -> tab)
    }
    ds.foreach {
      t =>
        val l = t.events.length
        if (l != 0) {
          t.events.last.time
          val start_trace = t.events.head.time
          val user = t.user
          val events = t.events
          events.foreach { e =>
            val p = e.point
            val j = math.floor((p.x - dimensions._3.x) / cellSize.meters).toInt
            val i = math.floor((p.y - dimensions._3.y) / cellSize.meters).toInt
            val h = e.time.toDateTime.getHourOfDay
            val mat = outputMap(user)
            synchronized(mat(h).inc(i, j))
            synchronized(outputMap += (user -> mat))

          }
        }
    }
    outputMap
  }


  def formDaysMatrices(ds: DataFrame[Trace], dimensions: (Int, Int, Point), cellSize: Distance): scala.collection.immutable.Map[String, Array[MatrixLight[Int]]] = {

    var outputMap: scala.collection.immutable.Map[String, Array[MatrixLight[Int]]] = scala.collection.immutable.Map[String, Array[MatrixLight[Int]]]().empty
    for (key <- ds.keys) {
      val tab2 = new Array[MatrixLight[Int]](7)
      for (i <- 0 to 6) tab2(i) = new MatrixLight[Int](dimensions._1, dimensions._2)
      outputMap += (key -> tab2)
    }
    ds.foreach {
      t =>
        val l = t.events.length
        if (l != 0) {
          t.events.last.time
          val start_trace = t.events.head.time
          val user = t.user
          val events = t.events
          events.foreach { e =>
            val p = e.point
            val j = math.floor((p.x - dimensions._3.x) / cellSize.meters).toInt
            val i = math.floor((p.y - dimensions._3.y) / cellSize.meters).toInt
            val day = e.time.toDateTime.getDayOfWeek - 1
            val mat = outputMap(user)
            mat(day).inc(i, j)
            outputMap += (user -> mat)

          }
        }
    }
    outputMap
  }

  def formWeekendsMatrices(ds: DataFrame[Trace], dimensions: (Int, Int, Point), cellSize: Distance): scala.collection.immutable.Map[String, Array[MatrixLight[Int]]] = {

    var outputMap: scala.collection.immutable.Map[String, Array[MatrixLight[Int]]] = scala.collection.immutable.Map[String, Array[MatrixLight[Int]]]().empty
    for (key <- ds.keys) {
      val tab2 = new Array[MatrixLight[Int]](2)
      for (i <- 0 to 1) tab2(i) = new MatrixLight[Int](dimensions._1, dimensions._2)
      outputMap += (key -> tab2)
    }
    ds.foreach {
      t =>
        val l = t.events.length
        if (l != 0) {
          t.events.last.time
          val start_trace = t.events.head.time
          val user = t.user
          val events = t.events
          events.foreach { e =>
            val p = e.point
            val j = math.floor((p.x - dimensions._3.x) / cellSize.meters).toInt
            val i = math.floor((p.y - dimensions._3.y) / cellSize.meters).toInt
            val day = e.time.toDateTime.getDayOfWeek
            val ind = if (day == DateTimeConstants.SUNDAY || day == DateTimeConstants.SATURDAY) 0 else 1
            val mat = outputMap(user)
            synchronized(mat(ind).inc(i, j))
            synchronized(outputMap += (user -> mat))
          }
        }
    }
    outputMap
  }


  def formWeekEndOnlyMatrice(ds: DataFrame[Trace], dimensions: (Int, Int, Point), cellSize: Distance): scala.collection.immutable.Map[String, MatrixLight[Int]] = {

    var trainOutputMap: scala.collection.immutable.Map[String, MatrixLight[Int]] = scala.collection.immutable.Map[String, MatrixLight[Int]]().empty
    var outputMap: scala.collection.immutable.Map[String, MatrixLight[Int]] = scala.collection.immutable.Map[String, MatrixLight[Int]]().empty
    for (key <- ds.keys) {
      outputMap += (key -> new MatrixLight[Int](dimensions._1, dimensions._2))
    }
    ds.foreach {
      t =>
        val l = t.events.length
        if (l != 0) {
          //println(s"LENGHT = $l")
          t.events.last.time
          val start_trace = t.events.head.time
          val user = t.user
          val events = t.events
          events.foreach { e =>
            val day = e.time.toDateTime.getDayOfWeek
            if (day == DateTimeConstants.SUNDAY && day == DateTimeConstants.SATURDAY) {
              val p = e.point
              val j = math.floor((p.x - dimensions._3.x) / cellSize.meters).toInt
              val i = math.floor((p.y - dimensions._3.y) / cellSize.meters).toInt
              val mat = outputMap(user)
              synchronized(mat.inc(i, j))
              synchronized(outputMap += (user -> mat))
            }
          }

        }
    }
    outputMap
  }


  def formWeekOnlyMatrice(ds: DataFrame[Trace], dimensions: (Int, Int, Point), cellSize: Distance): scala.collection.immutable.Map[String, MatrixLight[Int]] = {

    var outputMap: scala.collection.immutable.Map[String, MatrixLight[Int]] = scala.collection.immutable.Map[String, MatrixLight[Int]]().empty
    for (key <- ds.keys) {
      outputMap += (key -> new MatrixLight[Int](dimensions._1, dimensions._2))
    }
    // println(s"DIMENSIONS = $dimensions")
    ds.foreach {
      t =>
        val l = t.events.length
        if (l != 0) {
          //println(s"LENGHT = $l")
          t.events.last.time
          val start_trace = t.events.head.time
          val user = t.user
          val events = t.events
          events.foreach { e =>
            val day = e.time.toDateTime.getDayOfWeek
            if (day != DateTimeConstants.SUNDAY && day != DateTimeConstants.SATURDAY) {
              val p = e.point
              val j = math.floor((p.x - dimensions._3.x) / cellSize.meters).toInt
              val i = math.floor((p.y - dimensions._3.y) / cellSize.meters).toInt

              val mat = outputMap(user)
              synchronized(mat.inc(i, j))
              synchronized(outputMap += (user -> mat))

            }
          }

        }
    }
    outputMap
  }


  def formWeekDaysOnlyMatrices(ds: DataFrame[Trace], dimensions: (Int, Int, Point), cellSize: Distance): scala.collection.immutable.Map[String, Array[MatrixLight[Int]]] = {

    var outputMap: scala.collection.immutable.Map[String, Array[MatrixLight[Int]]] = scala.collection.immutable.Map[String, Array[MatrixLight[Int]]]().empty
    for (key <- ds.keys) {
      val tab2 = new Array[MatrixLight[Int]](2)
      for (i <- 0 to 1) tab2(i) = new MatrixLight[Int](dimensions._1, dimensions._2)
      outputMap += (key -> tab2)
    }

    ds.foreach {
      t =>
        val l = t.events.length
        if (l != 0) {
          t.events.last.time
          val start_trace = t.events.head.time
          val user = t.user
          val events = t.events
          events.foreach { e =>
            val p = e.point
            val j = math.floor((p.x - dimensions._3.x) / cellSize.meters).toInt
            val i = math.floor((p.y - dimensions._3.y) / cellSize.meters).toInt
            val day = e.time.toDateTime.getDayOfWeek
            val ind = if (day == DateTimeConstants.SUNDAY || day == DateTimeConstants.SATURDAY) 0 else 1
            val mat = outputMap(user)
            synchronized(mat(ind).inc(i, j))
            synchronized(outputMap += (user -> mat))
          }
        }
    }
    outputMap
  }


  def printMatrice[U](mat: Map[String, MatrixLight[U]]): Unit = {
    mat.foreach { m =>
      println(s"user : ${m._1} - Nb_(i,j) = ${m._2.data.keys.size}")
      println("-------------------------")
      if (m._2.nbCol > 1000 || m._2.nbRow > 1000) m._2.printNoneEmptyCells() else m._2.printMatrix()
      println("-------------------------")

    }
  }


  def computeMatricesSize(p1: Point, p2: Point, cellSize: Distance): (Int, Int, Point) = {


    val topCornerleft = Point(math.min(p1.x, p2.x), math.max(p1.y, p2.y))
    //val topCornerRight = Point(math.max(p1.x, p2.x), math.max(p1.y, p2.y))
    val bottomCornerleft = Point(math.min(p1.x, p2.x), math.min(p1.y, p2.y))
    val bottomCornerRight = Point(math.max(p1.x, p2.x), math.min(p1.y, p2.y))


    val width = bottomCornerleft.distance(bottomCornerRight)
    // println(s"Width = $width")
    val height = topCornerleft.distance(bottomCornerleft)
    // println(s"Height = $height")
    val nbRows = math.ceil(height / cellSize).toInt
    val nbColumn = math.ceil(width / cellSize).toInt

    (nbRows, nbColumn, bottomCornerleft)
  }


  def printMatrixCsvfile[U: Numeric](mat: immutable.Map[String, MatrixLight[U]], suffix: String, saveDir: String): Unit = {

    mat.foreach { m =>
      val bw = new BufferedWriter(new FileWriter(new File(s"$saveDir/${m._1}_$suffix.csv")))
      val matrix = m._2.proportional
      var str = "i\\j"

      for (j <- 0 to (matrix.nbCol - 1)) str += s" , $j"
      str += "\n"
      bw.write(str)
      for (i <- 0 to (matrix.nbRow - 1)) {
        str = s"$i"

        for (j <- 0 to (matrix.nbCol - 1)) {
          str += s", ${matrix(i, j)}"
        }
        str += "\n"
        bw.write(str)
      }
      bw.close()
    }
  }

  def printMatrixMutipleCsvfile[U: Numeric](mat: immutable.Map[String, Array[MatrixLight[U]]], suffix: String, saveDir: String): Unit = {

    mat.foreach { m =>
      for (k <- m._2.indices) {
        val bw = new BufferedWriter(new FileWriter(new File(s"$saveDir/${m._1}_${k}_$suffix.csv")))
        val matrix = m._2(k).proportional
        var str = "i\\j"
        println(s"${m._1}_$k.csv")
        for (j <- 0 to (matrix.nbCol - 1)) str += s" , $j"
        str += "\n"
        bw.write(str)
        for (i <- 0 to (matrix.nbRow - 1)) {
          str = s"$i"

          for (j <- 0 to (matrix.nbCol - 1)) {
            str += s", ${matrix(i, j)}"
          }
          str += "\n"
          bw.write(str)
        }
        bw.close()
      }
    }
  }

  def printMatrixCsvText[U: Numeric](mat: immutable.Map[String, MatrixLight[U]]): Unit = {


    mat.foreach { m =>
      println(s"user : ${m._1}")
      println("-------------------------")
      val matrix = m._2.normalizePositiveMatrix
      print("i\\j")
      for (j <- 0 to (matrix.nbCol - 1)) print(s" , $j")
      println("")
      //  m._2 foreach { row => row foreach print; println }
      for (i <- 0 to (matrix.nbRow - 1)) {
        print(s"$i")
        for (j <- 0 to (matrix.nbCol - 1)) {
          print(s", ${matrix(i, j)}")
        }
        println("")
      }
      println("-------------------------")

    }
  }


  def initializePoint(in: HeatMapGeneratorIn): (Point, Point) = {
    var p1 = Point(0, 0)
    var p2 = Point(0, 0)
    in.lat2 -> in.lng2 match {
      case (Some(lt), Some(lg)) =>
        p1 = LatLng.degrees(in.lat1, in.lng1).toPoint
        p2 = LatLng.degrees(lt, lg).toPoint

      case _ =>
        p1 = LatLng.degrees(in.lat1, in.lng1).toPoint

        p2 = p1.translate(S1Angle.degrees(in.ang), in.diag)

    }
    p1 -> p2
  }

  def emptyCellsRatio[U: Numeric](map: scala.collection.immutable.Map[String, MatrixLight[U]]): Double = {
    val tab: Array[Double] = Array.fill[Double](map.keys.size)(0.0)
    var ind: Int = 0
    map.foreach {
      case (k: String, mat: MatrixLight[U]) =>
        tab(ind) = mat.meanZero()
        // println(s" $ind = ${tab(ind)}")
        ind = ind + 1
    }
    val mean = tab.sum / tab.length.toDouble
    mean
  }


  def emptyCellsRatioMultiple[U: Numeric](map: scala.collection.immutable.Map[String, Array[MatrixLight[U]]]): Double = {
    val tab: Array[Double] = Array.fill[Double](map.keys.size)(0.0)
    var ind: Int = 0
    map.foreach {
      case (k: String, mat: Array[MatrixLight[U]]) =>
        mat.foreach {
          m => tab(ind) += m.meanZero()
        }
        tab(ind) = tab(ind) / mat.length
        ind = ind + 1
    }
    val mean = tab.sum / tab.length.toDouble
    mean
  }

  def restrictArea(ds: DataFrame[Trace], p1: Point, p2: Point): (DataFrame[Trace], Double) = {
    // Prepare the restrictive box
    val bounder = BoundingBox(p1, p2)
    // Restrict the tracers to the region.
    val output: DataFrame[Trace] = ds.map { t =>
      val newt = t.filter { e => bounder.contains(e.point) }
      newt
    }
    var nbTot = 0L
    var nbTaken = 0L
    ds.foreach(t => nbTot += t.events.size)
    output.foreach(t => nbTaken += t.events.size)
    val ratio = nbTaken.toDouble / nbTot.toDouble
    //println(s"ratio = $ratio, nbtaken = $nbTaken ,  nbtot = $nbTot  ,   outcount =  ${output.count().toDouble}  ,  ds count = ${ds.count().toDouble}")
    //ratio = output.count().toDouble/ds.count().toDouble
    (output, ratio)
  }


  def datasetStats(ds: DataFrame[Trace]): immutable.Map[String, Int] = {


    var stats = immutable.Map[String, Int]()
    ds.foreach {
      t =>
        if (t.events.nonEmpty) {
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
            } else {
              // save the day trace
              if (dayTrace.nonEmpty) {
                tracesList = tracesList :+ dayTrace
                tracesList = tracesList.sortBy(_.size)
              }
              day = newDay
              dayTrace = Seq[Event](e)
            }
            if (i == events.indices.last) {
              // save the day trace
              if (dayTrace.nonEmpty) {
                tracesList = tracesList :+ dayTrace
                tracesList = tracesList.sortBy(_.size)
              }
            }
          }

          tracesList = tracesList.sortBy(_.head.time.toDateTime.getDayOfYear)
          for (k <- tracesList.indices) {
            val l = tracesList(k)
            stats += s"${user}_$k" -> l.size
          }
        }
    }
    stats
  }


}


case class HeatMapGeneratorIn(

                               @Arg(help = "heatmaps")
                               heatmaps: Boolean = true,
                               @Arg(help = "Input train dataset")
                               data: Dataset,
                               @Arg(help = "Diagonal size of the restriction area")
                               diag: Distance = Distance.meters(250000),
                               @Arg(help = "Diagona angle (degrees) of the restriction area")
                               ang: Double = 45.0,
                               @Arg(help = "Type of distance metrics between matrices")
                               distanceType: Int = 2,
                               @Arg(help = "Cell Size in meters")
                               cellSize: Distance,
                               @Arg(help = "Lower point latitude")
                               lat1: Double = -61.0,
                               @Arg(help = "Lower point longitude")
                               lng1: Double = -131.0,
                               @Arg(help = "Higher point latitude (override diag & angle)")
                               lat2: Option[Double],
                               @Arg(help = "Higher point latitude (override diag & angle)")
                               lng2: Option[Double]

                             )

case class HeatMapGeneratorOut(
                                @Arg(help = "Stats for train dataset")
                                stats: immutable.Map[String, Int],
                                @Arg(help = "Length traces in train")
                                length: immutable.Map[String, Int],
                                @Arg(help = "For each user give LatLng with the most records")
                                maxCellCenter: immutable.Map[String, String]


                              )

/*
case class MatrixLight (
                        data : Map[(Int,Int),Double]
                         )

*/


