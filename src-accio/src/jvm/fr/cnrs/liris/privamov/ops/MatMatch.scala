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
// OLD VERSION
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
  category = "Attack",
  help = "Re-identification attack based on the matrix matching")
class MatMatchOp extends Operator[MatMatchIn, MatMatchOut] with SparkleOperator {


  override def execute(in: MatMatchIn, ctx: OpContext): MatMatchOut = {
    // read the data
    val ds = read(in.data, env)
    // rectangular point
    val (p1, p2) = initializePoint(in)
    val (rds, ratio) = restrictArea(ds, p1, p2)
    val dimensions = computeMatricesSize(p1, p2, in.cellSize)
    val (rate, userMatches, meantrain) =  reIdentAttack(rds,dimensions,in)

    MatMatchOut(userMatches, ratio, meantrain, rate)
  }

  def reIdentAttack(rds : DataFrame[Trace] ,dimensions : (Int , Int , Point) , in : MatMatchIn) : (Double , immutable.Map[String,String] ,Double) = {
   val ( (rate , matches), meantrain ) = in.matrixType match {
      case "full" => {
        val (trainOutputMap, testOutputMap) = formSingleMatrices(rds, in.divider, dimensions, in.cellSize)
          emptyCellsRatio(trainOutputMap)
        (reIdent(trainOutputMap, testOutputMap, in.distanceType),  emptyCellsRatio(trainOutputMap) )
          }
      case "days" => {
        val (trainOutputMap, testOutputMap) = formDaysMatrices(rds, in.divider, dimensions, in.cellSize)
        (reIdentMutiple(trainOutputMap, testOutputMap, in.distanceType) , emptyCellsRatioMultiple(trainOutputMap))
      }
      case "hours" => {
        val (trainOutputMap, testOutputMap) = formDayHoursMatrices(rds, in.divider, dimensions, in.cellSize)
        (reIdentMutiple(trainOutputMap, testOutputMap, in.distanceType) , emptyCellsRatioMultiple(trainOutputMap))
      }
      case "week_weekend" => {
        val (trainOutputMap, testOutputMap) = formWeekendsMatrices(rds, in.divider, dimensions, in.cellSize)
        (reIdentMutiple(trainOutputMap, testOutputMap, in.distanceType) , emptyCellsRatioMultiple(trainOutputMap))
      }
    case "weekOnly" => {
      val (trainOutputMap, testOutputMap) = formWeekOnlyMatrice(rds, in.divider, dimensions, in.cellSize)
      (reIdent(trainOutputMap, testOutputMap, in.distanceType) , emptyCellsRatio(trainOutputMap))
    }
      case "weekEndOnly" => {
        val (trainOutputMap, testOutputMap) = formWeekEndOnlyMatrice(rds, in.divider, dimensions, in.cellSize)
        (reIdent(trainOutputMap, testOutputMap, in.distanceType) , emptyCellsRatio(trainOutputMap))
      }
  }

    (rate , matches ,meantrain)
  }

  def formSingleMatrices(ds: DataFrame[Trace], divider: Int, dimensions: (Int, Int, Point), cellSize: Distance): (scala.collection.immutable.Map[String, MatrixLight[Int]], scala.collection.immutable.Map[String, MatrixLight[Int]]) = {

    var trainOutputMap: scala.collection.immutable.Map[String, MatrixLight[Int]] = scala.collection.immutable.Map[String, MatrixLight[Int]]().empty
    var testOutputMap: scala.collection.immutable.Map[String, MatrixLight[Int]] = scala.collection.immutable.Map[String, MatrixLight[Int]]().empty
    for (key <- ds.keys) {
      trainOutputMap += (key -> new MatrixLight[Int](dimensions._1, dimensions._2))
      testOutputMap += (key -> new MatrixLight[Int](dimensions._1, dimensions._2))
    }
    //printMatrice(trainOutputMap)
    // println(s" ${trainOutputMap("aysmsla").data.keys.size} " )
    println(s"DIMENSIONS = $dimensions")
    ds.foreach {
      t =>
        val l = t.events.length
        if (l != 0) {
          //println(s"LENGHT = $l")
          t.events.last.time
          val start_trace = t.events.head.time
          val durationTrain = t.duration.minus(t.duration.dividedBy(divider))
          val timeTestStart = start_trace.plus(durationTrain)
          val user = t.user
          val events = t.events
          events.foreach { e =>
            val p = e.point
            val j = math.floor((p.x - dimensions._3.x) / cellSize.meters).toInt
            val i = math.floor((p.y - dimensions._3.y) / cellSize.meters).toInt
            // println(s"($i,$j)")
            // if(i<0 || j < 0 )  println(s"p = $p ")
            if (e.time.isBefore(timeTestStart)) {
              val mat = trainOutputMap(user)
              mat.inc(i, j)
              trainOutputMap += (user -> mat)
            }
            else {
              val mat = testOutputMap(user)
              mat.inc(i, j)
              testOutputMap += (user -> mat)

            }
          }
        }
    }
    (trainOutputMap, testOutputMap)
  }

  def formDayHoursMatrices(ds: DataFrame[Trace], divider: Int, dimensions: (Int, Int, Point), cellSize: Distance): (scala.collection.immutable.Map[String, Array[MatrixLight[Int]]], scala.collection.immutable.Map[String, Array[MatrixLight[Int]]]) = {

    var trainOutputMap: scala.collection.immutable.Map[String, Array[MatrixLight[Int]]] = scala.collection.immutable.Map[String, Array[MatrixLight[Int]]]().empty
    var testOutputMap: scala.collection.immutable.Map[String, Array[MatrixLight[Int]]] = scala.collection.immutable.Map[String, Array[MatrixLight[Int]]]().empty
    for (key <- ds.keys) {
      val tab = new Array[MatrixLight[Int]](24)
      for(i <- 0 to  23 ) tab(i)= new MatrixLight[Int](dimensions._1, dimensions._2)
      trainOutputMap += (key -> tab)
      val tab2 = new Array[MatrixLight[Int]](24)
      for(i <- 0 to 23 ) tab2(i)= new MatrixLight[Int](dimensions._1, dimensions._2)
      testOutputMap += (key -> tab2)
    }

    println(s"DIMENSIONS = $dimensions")
    ds.foreach {
      t =>
        val l = t.events.length
        if (l != 0) {
          //println(s"LENGHT = $l")
          t.events.last.time
          val start_trace = t.events.head.time
          val durationTrain = t.duration.minus(t.duration.dividedBy(divider))
          val timeTestStart = start_trace.plus(durationTrain)
          val user = t.user
          val events = t.events
          events.foreach { e =>
            val p = e.point
            val j = math.floor((p.x - dimensions._3.x) / cellSize.meters).toInt
            val i = math.floor((p.y - dimensions._3.y) / cellSize.meters).toInt
            // println(s"($i,$j)")
            // if(i<0 || j < 0 )  println(s"p = $p ")
            val h = e.time.toDateTime.getHourOfDay
            //println(s" day = $day")
            if (e.time.isBefore(timeTestStart)) {
              val mat = trainOutputMap(user)
              mat(h).inc(i, j)
              trainOutputMap += (user -> mat)
            }
            else {
              val mat = testOutputMap(user)
              mat(h).inc(i, j)
              testOutputMap += (user -> mat)

            }
          }
        }
    }
    (trainOutputMap, testOutputMap)
  }


  def formDaysMatrices(ds: DataFrame[Trace], divider: Int, dimensions: (Int, Int, Point), cellSize: Distance): (scala.collection.immutable.Map[String, Array[MatrixLight[Int]]], scala.collection.immutable.Map[String, Array[MatrixLight[Int]]]) = {

    var trainOutputMap: scala.collection.immutable.Map[String, Array[MatrixLight[Int]]] = scala.collection.immutable.Map[String, Array[MatrixLight[Int]]]().empty
    var testOutputMap: scala.collection.immutable.Map[String, Array[MatrixLight[Int]]] = scala.collection.immutable.Map[String, Array[MatrixLight[Int]]]().empty
    for (key <- ds.keys) {
      val tab = new Array[MatrixLight[Int]](7)
      for(i <- 0 to  6 ) tab(i)= new MatrixLight[Int](dimensions._1, dimensions._2)
      trainOutputMap += (key -> tab)
      val tab2 = new Array[MatrixLight[Int]](7)
      for(i <- 0 to 6 ) tab2(i)= new MatrixLight[Int](dimensions._1, dimensions._2)
      testOutputMap += (key -> tab2)
    }

    println(s"DIMENSIONS = $dimensions")
    ds.foreach {
      t =>
        val l = t.events.length
        if (l != 0) {
          //println(s"LENGHT = $l")
          t.events.last.time
          val start_trace = t.events.head.time
          val durationTrain = t.duration.minus(t.duration.dividedBy(divider))
          val timeTestStart = start_trace.plus(durationTrain)
          val user = t.user
          val events = t.events
          events.foreach { e =>
            val p = e.point
            val j = math.floor((p.x - dimensions._3.x) / cellSize.meters).toInt
            val i = math.floor((p.y - dimensions._3.y) / cellSize.meters).toInt
            // println(s"($i,$j)")
            // if(i<0 || j < 0 )  println(s"p = $p ")
            val day = e.time.toDateTime.getDayOfWeek - 1
            //println(s" day = $day")
            if (e.time.isBefore(timeTestStart)) {
              val mat = trainOutputMap(user)
               if(mat(day) == null ) println("Nulllllllllllllllll")
              mat(day).inc(i, j)
              trainOutputMap += (user -> mat)
            }
            else {
              val mat = testOutputMap(user)
              mat(day).inc(i, j)
              testOutputMap += (user -> mat)

            }
          }
        }
    }
    (trainOutputMap, testOutputMap)
  }

  def formWeekendsMatrices(ds: DataFrame[Trace], divider: Int, dimensions: (Int, Int, Point), cellSize: Distance): (scala.collection.immutable.Map[String, Array[MatrixLight[Int]]], scala.collection.immutable.Map[String, Array[MatrixLight[Int]]]) = {

    var trainOutputMap: scala.collection.immutable.Map[String, Array[MatrixLight[Int]]] = scala.collection.immutable.Map[String, Array[MatrixLight[Int]]]().empty
    var testOutputMap: scala.collection.immutable.Map[String, Array[MatrixLight[Int]]] = scala.collection.immutable.Map[String, Array[MatrixLight[Int]]]().empty
    for (key <- ds.keys) {
      val tab = new Array[MatrixLight[Int]](2)
      for(i <- 0 to 1 ) tab(i)= new MatrixLight[Int](dimensions._1, dimensions._2)
      trainOutputMap += (key -> tab)
      val tab2 = new Array[MatrixLight[Int]](2)
      for(i <- 0 to 1 ) tab2(i)= new MatrixLight[Int](dimensions._1, dimensions._2)
      testOutputMap += (key -> tab2)
    }

    println(s"DIMENSIONS = $dimensions")
    ds.foreach {
      t =>
        val l = t.events.length
        if (l != 0) {
          //println(s"LENGHT = $l")
          t.events.last.time
          val start_trace = t.events.head.time
          val durationTrain = t.duration.minus(t.duration.dividedBy(divider))
          val timeTestStart = start_trace.plus(durationTrain)
          val user = t.user
          val events = t.events
          events.foreach { e =>
            val p = e.point
            val j = math.floor((p.x - dimensions._3.x) / cellSize.meters).toInt
            val i = math.floor((p.y - dimensions._3.y) / cellSize.meters).toInt
            // println(s"($i,$j)")
            // if(i<0 || j < 0 )  println(s"p = $p ")
            val day = e.time.toDateTime.getDayOfWeek
            val ind =  if(day == DateTimeConstants.SUNDAY || day == DateTimeConstants.SATURDAY) 0 else 1
            if (e.time.isBefore(timeTestStart)) {
              val mat = trainOutputMap(user)
              mat(ind).inc(i, j)
              trainOutputMap += (user -> mat)
            }
            else {
              val mat = testOutputMap(user)
              mat(ind).inc(i, j)
              testOutputMap += (user -> mat)

            }
          }
        }
    }
    (trainOutputMap, testOutputMap)
  }


  def formWeekEndOnlyMatrice(ds: DataFrame[Trace], divider: Int, dimensions: (Int, Int, Point), cellSize: Distance): (scala.collection.immutable.Map[String, MatrixLight[Int]], scala.collection.immutable.Map[String, MatrixLight[Int]]) = {

    var trainOutputMap: scala.collection.immutable.Map[String, MatrixLight[Int]] = scala.collection.immutable.Map[String, MatrixLight[Int]]().empty
    var testOutputMap: scala.collection.immutable.Map[String, MatrixLight[Int]] = scala.collection.immutable.Map[String, MatrixLight[Int]]().empty
    for (key <- ds.keys) {
      trainOutputMap += (key -> new MatrixLight[Int](dimensions._1, dimensions._2))
      testOutputMap += (key -> new MatrixLight[Int](dimensions._1, dimensions._2))
    }
    //printMatrice(trainOutputMap)
    // println(s" ${trainOutputMap("aysmsla").data.keys.size} " )
    println(s"DIMENSIONS = $dimensions")
    ds.foreach {
      t =>
        val l = t.events.length
        if (l != 0) {
          //println(s"LENGHT = $l")
          t.events.last.time
          val start_trace = t.events.head.time
          val durationTrain = t.duration.minus(t.duration.dividedBy(divider))
          val timeTestStart = start_trace.plus(durationTrain)
          val user = t.user
          val events = t.events
          events.foreach { e =>
            val day = e.time.toDateTime.getDayOfWeek
            if(day == DateTimeConstants.SUNDAY && day == DateTimeConstants.SATURDAY) {
              val p = e.point
              val j = math.floor((p.x - dimensions._3.x) / cellSize.meters).toInt
              val i = math.floor((p.y - dimensions._3.y) / cellSize.meters).toInt
              // println(s"($i,$j)")
              // if(i<0 || j < 0 )  println(s"p = $p ")
              if (e.time.isBefore(timeTestStart)) {
                val mat = trainOutputMap(user)
                mat.inc(i, j)
                trainOutputMap += (user -> mat)
              }
              else {
                val mat = testOutputMap(user)
                mat.inc(i, j)
                testOutputMap += (user -> mat)

              }
            }
          }

        }
    }
    (trainOutputMap, testOutputMap)
  }




  def formWeekOnlyMatrice(ds: DataFrame[Trace], divider: Int, dimensions: (Int, Int, Point), cellSize: Distance): (scala.collection.immutable.Map[String, MatrixLight[Int]], scala.collection.immutable.Map[String, MatrixLight[Int]]) = {

    var trainOutputMap: scala.collection.immutable.Map[String, MatrixLight[Int]] = scala.collection.immutable.Map[String, MatrixLight[Int]]().empty
    var testOutputMap: scala.collection.immutable.Map[String, MatrixLight[Int]] = scala.collection.immutable.Map[String, MatrixLight[Int]]().empty
    for (key <- ds.keys) {
      trainOutputMap += (key -> new MatrixLight[Int](dimensions._1, dimensions._2))
      testOutputMap += (key -> new MatrixLight[Int](dimensions._1, dimensions._2))
    }
    //printMatrice(trainOutputMap)
    // println(s" ${trainOutputMap("aysmsla").data.keys.size} " )
    println(s"DIMENSIONS = $dimensions")
    ds.foreach {
      t =>
        val l = t.events.length
        if (l != 0) {
          //println(s"LENGHT = $l")
          t.events.last.time
          val start_trace = t.events.head.time
          val durationTrain = t.duration.minus(t.duration.dividedBy(divider))
          val timeTestStart = start_trace.plus(durationTrain)
          val user = t.user
          val events = t.events
          events.foreach { e =>
            val day = e.time.toDateTime.getDayOfWeek
        if(day != DateTimeConstants.SUNDAY && day != DateTimeConstants.SATURDAY) {
            val p = e.point
            val j = math.floor((p.x - dimensions._3.x) / cellSize.meters).toInt
            val i = math.floor((p.y - dimensions._3.y) / cellSize.meters).toInt
            // println(s"($i,$j)")
            // if(i<0 || j < 0 )  println(s"p = $p ")
            if (e.time.isBefore(timeTestStart)) {
              val mat = trainOutputMap(user)
              mat.inc(i, j)
              trainOutputMap += (user -> mat)
            }
            else {
              val mat = testOutputMap(user)
              mat.inc(i, j)
              testOutputMap += (user -> mat)

            }
          }
          }

        }
    }
    (trainOutputMap, testOutputMap)
  }


  def formWeekDaysOnlyMatrices(ds: DataFrame[Trace], divider: Int, dimensions: (Int, Int, Point), cellSize: Distance): (scala.collection.immutable.Map[String, Array[MatrixLight[Int]]], scala.collection.immutable.Map[String, Array[MatrixLight[Int]]]) = {

    var trainOutputMap: scala.collection.immutable.Map[String, Array[MatrixLight[Int]]] = scala.collection.immutable.Map[String, Array[MatrixLight[Int]]]().empty
    var testOutputMap: scala.collection.immutable.Map[String, Array[MatrixLight[Int]]] = scala.collection.immutable.Map[String, Array[MatrixLight[Int]]]().empty
    for (key <- ds.keys) {
      val tab = new Array[MatrixLight[Int]](2)
      for(i <- 0 to 1 ) tab(i)= new MatrixLight[Int](dimensions._1, dimensions._2)
      trainOutputMap += (key -> tab)
      val tab2 = new Array[MatrixLight[Int]](2)
      for(i <- 0 to 1 ) tab2(i)= new MatrixLight[Int](dimensions._1, dimensions._2)
      testOutputMap += (key -> tab2)
    }

    println(s"DIMENSIONS = $dimensions")
    ds.foreach {
      t =>
        val l = t.events.length
        if (l != 0) {
          //println(s"LENGHT = $l")
          t.events.last.time
          val start_trace = t.events.head.time
          val durationTrain = t.duration.minus(t.duration.dividedBy(divider))
          val timeTestStart = start_trace.plus(durationTrain)
          val user = t.user
          val events = t.events
          events.foreach { e =>
            val p = e.point
            val j = math.floor((p.x - dimensions._3.x) / cellSize.meters).toInt
            val i = math.floor((p.y - dimensions._3.y) / cellSize.meters).toInt
            // println(s"($i,$j)")
            // if(i<0 || j < 0 )  println(s"p = $p ")
            val day = e.time.toDateTime.getDayOfWeek
            val ind =  if(day == DateTimeConstants.SUNDAY || day == DateTimeConstants.SATURDAY) 0 else 1
            if (e.time.isBefore(timeTestStart)) {
              val mat = trainOutputMap(user)
              mat(ind).inc(i, j)
              trainOutputMap += (user -> mat)
            }
            else {
              val mat = testOutputMap(user)
              mat(ind).inc(i, j)
              testOutputMap += (user -> mat)

            }
          }
        }
    }
    (trainOutputMap, testOutputMap)
  }

  def reIdent(trainMats: immutable.Map[String, MatrixLight[Int]], testMats: immutable.Map[String, MatrixLight[Int]], n: Int): (Double, immutable.Map[String, String]) = {
   // printMatrixCsvfile(testMats)
    var matches: scala.collection.immutable.Map[String, String] = scala.collection.immutable.Map[String, String]()
    var nbMatches: Int = 0
    var trainDconv2Save = immutable.Map[String, (Array[Double],Array[Double],Double,Double) ]()
    var testDconv2Save = immutable.Map[String, (Array[Double],Array[Double],Double,Double) ]()
    if(n == -1 ){
      trainDconv2Save = computeDconv2Save(trainMats)
      testDconv2Save = computeDconv2Save(testMats)
    }
   // println("start foreach")
    testMats.par.foreach {
      //testMats.foreach {
      case (k: String, mat_k: MatrixLight[Int]) =>
       // println(s" user : $k")
      //  mat_k.proportional.printMatrix()
        var order = immutable.Map[String, Double]()
        trainMats.foreach {
          case (u: String, mat_u: MatrixLight[Int]) =>
            //println(s" computing : d($k,$u)")
            var dist = 0.0
            if(n == -1){
              val save1 = testDconv2Save(k)
              val save2 = trainDconv2Save(u)
              dist = dConv2(mat_k.proportional, mat_u.proportional, save1._1, save1._2 , save1._3 , save1._4,save1._2, save2._2 , save2._3 , save2._4)
            //  println(s"d($k,$u)=$dist")
            }else {
              dist = d(mat_k.proportional, mat_u.proportional, n)
            }
            order += (u -> dist )
        }
  /*    println("---------------------")
   println(s"order user $k : ")
         order.foreach{case (key,value) => println(s"$key , $value | ")}
       println("")
       println("---------------------")
*/
        val seq = if ( n == -1 ) {
        // val  closeToZero =  order.map{case (key : String ,valueKey : Double) => (key, math.abs(valueKey))}
        var  closeToZero =  order.filter{ case (key , value ) =>  value > 0.0 }
          if(closeToZero.isEmpty) closeToZero += "-"-> 0.0
          closeToZero.toSeq.sortBy(_._2).reverse
    }else {
      order.toSeq.sortBy(_._2)
    }
/*
       println("---------------------")
        println(s"SEQ user $k : ")
        seq.foreach{case (key,value) => println(s"$key , $value | ")}
        println("")
      println("---------------------")
*/
       synchronized(matches += (k -> seq.head._1))
        if (k == seq.head._1) nbMatches += 1
    }
   // println("end foreach")

    val rate: Double = nbMatches.toDouble / testMats.keys.size.toDouble
    //println(s"rate = $rate")
    //println(matches)
    (rate, matches)
  }

  def dConv2(m1: MatrixLight[Double], m2: MatrixLight[Double], d_i_1 : Array[Double], d_j_1 : Array[Double], avg1 : Double, auto1 : Double, d_i_2 : Array[Double], d_j_2 : Array[Double], avg2 : Double, auto2 : Double): Double= {
    var dist = 0.0
   val indices =  m1.data.keys.toSet ++ m1.data.keys.toSet
    indices.foreach{
      case (i,j) =>
    dist = dist + (m1(i, j) - d_i_1(i) - d_j_1(j) + avg1) * (m2(i, j) - d_i_2(i) - d_j_2(j) + avg2)
      }
    dist = dist / (m1.nbRow * m1.nbCol)
    dist = dist / math.sqrt(auto1*auto2)
    dist
  }

  def computeDconv2Save(mats: immutable.Map[String, MatrixLight[Int]]): immutable.Map[String, (Array[Double],Array[Double],Double,Double) ] ={
  var dconv2Save = immutable.Map[String, (Array[Double],Array[Double],Double,Double) ]()
  mats.foreach { case (k, mat) =>

    val m = mat.proportional
    var dc = 0.0
    val d_i_ = Array.fill[Double](m.nbRow)(0)
    val d_j_ = Array.fill[Double](m.nbCol)(0)
    var d = 0.0
    val indices = m.data.keys.toSet
    var set_i = Set[Int]()
    var set_j = Set[Int]()
    indices.foreach {
      case (i: Int, j: Int) =>
        set_i += i
        set_j += j
        d_i_(i) = d_i_(i) + m(i, j)
        d_j_(j) = d_j_(j) + m(i, j)
        d += m(i, j)
    }
    for (i <- set_i) {
      d_i_(i) = d_i_(i) / m.nbCol.toDouble
    }
    for (j <- set_j) {
      d_j_(j) = d_j_(j) / m.nbRow.toDouble
    }
    d = d / (m.nbRow * m.nbCol).toDouble
    for (i <- set_i) {
      for (j <- set_j) {
        dc = dc + (m(i, j) - d_i_(i) - d_j_(j) - d) * (m(i, j) - d_i_(i) - d_j_(j) - d)
      }
    }
    dc = dc / (m.nbRow * m.nbCol)
    dconv2Save +=   k -> (d_i_,d_j_,d,dc)
  }
  dconv2Save
}

  def reIdentMutiple(trainMats: immutable.Map[String, Array[MatrixLight[Int]]], testMats: immutable.Map[String, Array[MatrixLight[Int]]], n: Int): (Double, immutable.Map[String, String]) = {
   // printMatrixMutipleCsvfile(trainMats)

    var matches: scala.collection.immutable.Map[String, String] = scala.collection.immutable.Map[String, String]()
    var nbMatches: Int = 0
    println("start foreach")
    testMats.par.foreach {
      //testMats.foreach {
      case (k: String, mat_k: Array[MatrixLight[Int]]) =>
        var order = immutable.Map[String, Double]()
        trainMats.foreach {
          case (u: String, mat_u: Array[MatrixLight[Int]]) =>
            //order += (u -> d(mat_k.normalizePositiveMatrix, mat_u.normalizePositiveMatrix , n))
            //printf(s" computing : d($k,$u)")
            var dist = 0.0
            for(ind <- mat_k.indices){
              dist = dist + d(mat_k(ind).proportional, mat_u(ind).proportional, n)
            }
            order += (u -> dist)
        }

        val seq = order.toSeq.sortBy(_._2)
        // println(seq)
        //  println(s"  -  $k ")
       /* println("---------------------")
        println(s"SEQ user $k : ")
        seq.foreach{case (key,value) => println(s"$key , $value | ")}
        println("")
        println("---------------------")

*/


        synchronized(matches += (k -> seq.head._1))
        // matches += (k -> seq.head._1)
        //println("after ")
        if (k == seq.head._1) nbMatches += 1
    }
   // println("end foreach")

    val rate: Double = nbMatches.toDouble / testMats.keys.size.toDouble
    //println(s"rate = $rate")
    //println(matches)
    (rate, matches)
  }



  def printMatrice[U](mat: Map[String, MatrixLight[U]]): Unit = {
    mat.foreach { m =>
      println(s"user : ${m._1} - Nb_(i,j) = ${m._2.data.keys.size}")
      println("-------------------------")
      if (m._2.nbCol > 1000 || m._2.nbRow > 1000) m._2.printNoneEmptyCells() else m._2.printMatrix()
      println("-------------------------")

    }
  }

  def meanFilter(mat: Array[Array[Double]]): Array[Array[Double]] = {
    val newMat = Array.fill[Array[Double]](mat.length)(Array.fill[Double](mat(0).length)(0))
    for (i <- 1 to mat.length - 2) {
      for (j <- 1 to mat(i).length - 2) {
        for (ki <- -1 to 1) {
          for (kj <- -1 to 1) {
            newMat(i)(j) = newMat(i)(j) + mat(i + ki)(j + kj)
          }
        }
        newMat(i)(j) = newMat(i)(j) / 9
      }
    }
    newMat
  }

  def noneZeroCellsRatio[U](unknownMat: MatrixLight[U], knownMat: MatrixLight[U]): Double = {
    val nb1 = unknownMat.data.keys.size + knownMat.data.keys.size
    val nb2 = unknownMat.data.keys.toSet.intersect(knownMat.data.keys.toSet).size
   1 - nb2.toDouble / nb1.toDouble
  }


  def dNoneDet(unknownMat: MatrixLight[Double], knownMat: MatrixLight[Double], k: Int, x: Int): Double = {
    var d: Double = 0.0
    // random number generator
    val rnd = new Random()
    // all the non zeros indices
    val noneEmptyCellsArray: Array[(Int, Int)] = unknownMat.data.keys.toArray

    val k2 = Math.min(k, noneEmptyCellsArray.length)
    var tmpx = 0
    // repeat x times the process
    while (tmpx < x) {
      // this iteration chosen cells
      var chosenCells: Seq[(Int, Int)] = Seq[(Int, Int)]()
      var tmpK = 0
      val mask = Array.fill[Boolean](noneEmptyCellsArray.length)(false)
      while (tmpK < k2) {
        val ind: Int = rnd.nextInt(noneEmptyCellsArray.length)
        // println(s" tmpk/k =  $tmpK/$k2 ;  ind = $ind ; tmpx/x = $tmpx/$x ")
        if (!mask(ind)) {
          mask(ind) = true
          chosenCells = chosenCells :+ noneEmptyCellsArray(ind)
          tmpK += 1
        }
      }
      d = d + vectorDistance(chosenCells, unknownMat, knownMat)
      tmpx += 1
      // println(s"chosenCells = $chosenCells")
    }
    d = d / x
    d
  }

  def vectorDistance(chosenCells: Seq[(Int, Int)], unknownMat: MatrixLight[Double], knownMat: MatrixLight[Double]): Double = {
    var d: Double = 0.0
    chosenCells.foreach {
      ind =>
        d += Math.pow(unknownMat(ind._1, ind._2) - knownMat(ind._1, ind._2), 2)
    }
    d = math.sqrt(d)
    d
  }

  def d(m1: MatrixLight[Double], m2: MatrixLight[Double], typeD: Int): Double = {
    var d: Double = 0
    if (typeD > 0) {
      d = m1.minus(m2).norm(typeD)

    } else {
      typeD match {
        case -1 => d = dcov2(m1,m2) / math.sqrt(dcov2(m1,m1) *  dcov2(m2,m2))
        case -2 => d = dKL(m1, m2) // not symmetric
        case  0  => d = dNoneDet(m1, m2, 10, 10)
        case  -3 => d = noneZeroCellsRatio(m1, m2)
        case -11 => d = dLoren(m1,m2)
        case -24 => d = dInnerP(m1,m2)
        case -26 => d = dcosine(m1,m2)
        case -31 => d = dDice(m1,m2)
        case -42 => d = dNdiv(m1,m2)
        case -46 => d = dclark(m1,m2)
        case -51 => d = dtopsoe(m1,m2)
        case -53 => d = dJensenDiv(m1,m2)
      }
    }
    d
  }

  def dJensenDiv(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var top = 0.0
    var ln1 = 0.0
    var ln2 = 0.0
    var ln12 = 0.0

    var d1 = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>
        ln1 =  if(m1(i,j) == 0 ) 0 else  m1(i,j) * math.log(m1(i,j))
        ln2 =  if(m2(i,j) == 0 ) 0 else  m2(i,j) * math.log(m2(i,j))
        ln12 =  if(m1(i,j) == 0 && m2(i,j) == 0 ) 0 else  ((m1(i,j)+m2(i,j))/2) * math.log((m1(i,j)+m2(i,j))/2)
        d1 = d1 +   (ln1 + ln2)/2    -  ln12
    }
    d1
  }
  def dtopsoe(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var top = 0.0
    var ln1 = 0.0
    var ln2 = 0.0
    var d1 = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>
            ln1 =  if(m1(i,j) == 0 ) 0 else  m1(i,j) * math.log(2*m1(i,j)/(m1(i, j) + m2(i, j)))
        ln2 =  if(m2(i,j) == 0 ) 0 else  m2(i,j) * math.log(2*m2(i,j)/(m1(i, j) + m2(i, j)))

        d1 = d1 + ln1 + ln2
    }
    d1
  }
  def dDice(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var top = 0.0
    var low1 = 0.0
    var low2 = 0.0
    var d1 = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>
        top = top + (m1(i, j) - m2(i, j))*(m1(i, j) - m2(i, j))
        low1= low1 +  m1(i, j)*m1(i, j)
        low2= low2 + m2(i, j)*m2(i, j)
    }
    d1 =  top / (low1 + low2)
    d1
  }

  def dInnerP(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var d1  = 0.0
    val indices = m1.data.keys.toSet.intersect(m2.data.keys.toSet)
    indices.foreach {
      case (i: Int, j: Int) =>
        d1 = d1  + m1(i, j)*m2(i, j)
    }
     1-d1
  }

  def dNdiv(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var tmp = 0.0
    var d1  = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>

        tmp = (m1(i, j) + m2(i, j))*(m1(i, j) + m2(i, j))
        if( m1(i,j) != 0.0)   tmp = tmp/m1(i,j)
        else tmp = tmp / 1E-10
        d1 = d1  + tmp
    }
    d1
  }

  def dLoren(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var tmp  = 0.0
    var d1  = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>
        tmp = math.abs(m1(i, j) - m2(i, j))
        d1 = d1 +  math.log(1 + tmp )
    }
   d1
  }

  def dclark(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var top = 0.0
    var low = 0.0
    var d1  = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>
          top = math.abs(m1(i, j) - m2(i, j))
          low = m1(i, j) + m2(i, j)
          d1 = d1 + (top / low) * (top / low)

    }
    math.sqrt(d1)
  }


  def dcosine(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var d12 = 0.0
    var d11 = 0.0
    var d22 = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>
        d12 = d12 + m1(i, j)* m2(i, j)
        d11 = d11 + m1(i, j)* m1(i, j)
        d22 = d22 + m2(i, j)* m2(i, j)
    }
   1  -  d12 / math.sqrt(d11*d22)
  }

  def dcov2(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
     val nbRow = m1.nbRow
    val nbCol = m1.nbCol
    var dc = 0.0
    val d_i_1 = Array.fill[Double](nbRow)(0)
    val d_i_2 = Array.fill[Double](nbRow)(0)
    val d_j_1 = Array.fill[Double](nbCol)(0)
    val d_j_2 = Array.fill[Double](nbCol)(0)
    var d1 = 0.0
    var d2 = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    var set_i = Set[Int]()
    var set_j = Set[Int]()
    indices.foreach {
      case (i: Int, j: Int) =>
        set_i += i
        set_j += j
        d_i_1(i) = d_i_1(i) + m1(i, j)
        d_i_2(i) = d_i_2(i) + m2(i, j)
        d_j_1(j) = d_j_1(j) + m1(i, j)
        d_j_2(j) = d_j_2(j) + m2(i, j)
        d1 += m1(i, j)
        d2 += m2(i, j)
    }
    for (i <- set_i) {
      d_i_1(i) = d_i_1(i) / m1.nbCol.toDouble
      d_i_2(i) = d_i_2(i) / m2.nbCol.toDouble
    }
    for (j <- set_j) {
      d_j_1(j) = d_j_1(j) / m1.nbRow.toDouble
      d_j_2(j) = d_j_2(j) / m2.nbRow.toDouble
    }
    d1 = d1 / (nbRow * nbCol).toDouble
    d2 = d2 / (nbRow * nbCol).toDouble
    for (i <- set_i) {
      for (j <- set_j) {
        dc = dc + (m1(i, j) - d_i_1(i) - d_j_1(j) - d1) * (m2(i, j) - d_i_2(i) - d_j_2(j) - d2)
      }
    }
    dc = dc / (nbRow * nbCol)
    dc
  }


  def dKL(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var dc = 0.0
    var ln1 = 0.0
    var tmp = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>
        if (m1(i, j) ==  0) tmp = 0
        else {
         tmp =   if(m2(i,j) == 0 ) m1(i, j) * math.log(m1(i, j) / 1E-10 ) else m1(i, j) * math.log(m1(i, j) / m2(i, j))
        }
        dc = dc + tmp
    }
    dc
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


  def printMatrixCsvfile[U: Numeric](mat: immutable.Map[String, MatrixLight[U]]): Unit = {

    mat.foreach { m =>
      val bw = new BufferedWriter(new FileWriter(new File(s"${m._1}.csv")))
      val matrix = m._2.normalizePositiveMatrix
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
  def printMatrixMutipleCsvfile[U: Numeric](mat: immutable.Map[String, Array[MatrixLight[U]]]): Unit = {

    mat.foreach { m =>
      for (k <- m._2.indices) {
        val bw = new BufferedWriter(new FileWriter(new File(s"${m._1}_$k.csv")))
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


  def initializePoint(in: MatMatchIn): (Point, Point) = {
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
         mat.foreach{
          m  => tab(ind)+= m.meanZero()
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

}

case class MatMatchIn(
                       @Arg(help = "Input dataset")
                       data: Dataset,
                       @Arg(help = "Proportion of the test set in the form 1/divider ")
                       divider: Int,
                       @Arg(help = "Lower point latitude")
                       lat1: Double,
                       @Arg(help = "Lower point longitude")
                       lng1: Double,
                       @Arg(help = "Diagonal size of the restriction area")
                       diag: Distance = Distance.meters(250000),
                       @Arg(help = "Diagona angle (degrees) of the restriction area")
                       ang: Double = 45.0,
                       @Arg(help = "Higher point latitude (override diag & angle)")
                       lat2: Option[Double],
                       @Arg(help = "Higher point latitude (override diag & angle)")
                       lng2: Option[Double],
                       @Arg(help = "Type of distance metrics between matrices")
                       distanceType: Int = 2,
                       @Arg(help = "Type of matrix (temporal aspects)")
                       matrixType: String = "full",
                       @Arg(help = "Cell Size in meters")
                       cellSize: Distance,
                       @Arg(help = "Type of distance (see doc, default euclidean distance")
                       distType: Int = 2
                     )

case class MatMatchOut(
                        @Arg(help = "Matches between users") matches: immutable.Map[String, String],
                        @Arg(help = "Ratio points taken by the restriction area") ratioPointsTaken: Double,
                        @Arg(help = "Mean Ratio of empty cells in matrices") meanEmptyCells: Double,
                        @Arg(help = "Re-Ident rate") rate: Double
                      )

/*
case class MatrixLight (
                        data : Map[(Int,Int),Double]
                         )

*/

class MatrixLight[U: Numeric](nbRows: Int, nbColumn: Int) {
  val numeric = implicitly[Numeric[U]]

  def nbRow = nbRows

  def nbCol = nbColumn

  var data: scala.collection.mutable.Map[(Int, Int), U] = scala.collection.mutable.Map[(Int, Int), U]()

  def indices : Set[(Int,Int)] = data.keySet

  var denominator : U  = numeric.one // stored form proportional method
  def apply(i: Int, j: Int): U = {
    if (i >= nbRow || j >= nbColumn || i < 0 || j < 0) throw new ArrayIndexOutOfBoundsException
    val d = data.get(i -> j)
    d match {
      case Some(value) => value
      case None => numeric.zero
    }
  }

  def assign(i: Int, j: Int, d: U): Unit = {
   if(!numeric.equiv(d,numeric.zero)) data += (i, j) -> d
  }

  def remove(i:Int,j:Int) : Unit ={
    data = data - (i -> j)
  }
  def printMatrix(): Unit = {
    println("-------------------------")
    for (i <- 0 to this.nbRow - 1) {
      for (j <- 0 to (this.nbCol - 1)) {
        print(s"${this (i, j)} \t  ")
      }
      println("")
    }
    println("-------------------------")
  }

  def printNoneEmptyCells(): Unit = {
    this.foreachNoneEmpty {
      case ((i, j), v) =>
        println(s" ($i,$j) = $v")
    }
  }

  def inc(i: Int, j: Int): Unit = {
    val d = data.get(i -> j)
    d match {
      case Some(value) => data += (i, j) -> numeric.plus(value, numeric.one)
      case None => data += (i, j) -> numeric.one
    }
  }

  def min: U = {
    val d = if (data.isEmpty) numeric.zero else data.valuesIterator.min
    if (data.keys.size < nbRow * nbCol) {
      if (numeric.gt(d, numeric.zero)) numeric.zero
      else d
    } else d
  }

  def max: U = {

    val d = if (data.isEmpty) numeric.zero else data.valuesIterator.max
    if (data.keys.size < nbRow * nbCol) {
      if (numeric.gt(numeric.zero, d)) numeric.zero
      else d
    } else d
  }

  def sum: U = {
    var s = numeric.zero
    data.foreach {
      case ((i, j), value) =>
        s = numeric.plus(s, value)
    }
    s
  }


  def div(d: U): Unit = {
    data.transform {
      case ((i, j), value) =>
        numeric match {
          case num: Fractional[U] => num.div(value, d)
          case num: Integral[U] => num.quot(value, d)
        }
    }
  }


  def sub(d: U): Unit = {

    for (i <- 0 to (this.nbRow - 2)) {
      for (j <- 0 to (this.nbCol - 2)) {
        this.sub(i, j, d)
      }
    }
  }


  def add(d: U): Unit = {
    for (i <- 0 to (this.nbRow - 2)) {
      for (j <- 0 to (this.nbCol - 2)) {
        this.add(i, j, d)
      }
    }
  }

  def mul(d: U): Unit = {
    data.transform {
      case ((i, j), value) =>
        numeric.times(value, d)
    }
  }

  def div(i: Int, j: Int, divider: U): Unit = {
    val d = data.get(i -> j)
    d match {
      case Some(value) =>
        numeric match {
          case num: Fractional[U] => data += (i, j) -> num.div(value, divider)
          case num: Integral[U] => data += (i, j) -> num.quot(value, divider)
        }
      case None =>
    }
  }

  def mul(i: Int, j: Int, multiplier: U): Unit = {
    val d = data.get(i -> j)
    d match {
      case Some(value) => data += (i, j) -> numeric.times(value, multiplier)
      case None =>
    }
  }

  def add(i: Int, j: Int, db: U): Unit = {
    val d = data.get(i -> j)
    d match {
      case Some(value) => data += (i, j) -> numeric.plus(value, db)
      case None => data += (i, j) -> numeric.plus(numeric.zero, db)
    }
  }

  def sub(i: Int, j: Int, sb: U): Unit = {
    val d = data.get(i -> j)
    d match {
      case Some(value) => data += (i, j) -> numeric.minus(value, sb)
      case None => data += (i, j) -> numeric.minus(numeric.zero, sb)
    }
  }

  def count(f: ((U) => Boolean)): Int = {
    var cpt = data.count { case (_, value) => f(value) }
    if (f(numeric.zero)) cpt += (nbRow * nbCol - data.keys.size)
    cpt
  }

  def meanZero(): Double = {
    val cpt = this.count {
      case (value) => numeric.equiv(value, numeric.zero)
    }
    //println(s" cpt =${cpt.toDouble}  ,  mul = ${(nbColumn * nbRow).toDouble}")
    cpt.toDouble / (nbColumn * nbRow).toDouble
  }

  def avg(): Double = {
    numeric.toDouble(data.valuesIterator.sum) / data.valuesIterator.size.toDouble
  }

  def diff(m: MatrixLight[U], i: Int, j: Int): U = {
    numeric.minus(this (i, j), m(i, j))
  }

  def add(m: MatrixLight[U], i: Int, j: Int): U = {
    numeric.plus(this (i, j), m(i, j))
  }

  def transform(f: ((Int, Int), U) => U): MatrixLight[U] = {
    val newMat = new MatrixLight[U](this.nbRow, this.nbCol)
    for (i <- 0 to (this.nbRow - 1)) {
      for (j <- 0 to (this.nbCol - 1)) {
        newMat.assign(i, j, f((i, j), this (i, j)))
      }
    }
    newMat
  }

  def transformCells(cells : Set[(Int,Int)],f: ((Int, Int), U) => U): MatrixLight[U] = {
    val newMat = new MatrixLight[U](this.nbRow, this.nbCol)
     cells.foreach{ case (i : Int,j : Int) => newMat.assign(i, j, f((i, j), this (i, j))) }
    newMat
  }



  def foreachNoneEmpty(f: (((Int, Int), U)) => Unit): Unit = {
    data.foreach(f)
  }


  def foreach(f: (((Int, Int), U)) => Unit): Unit = {
    for (i <- 0 to (this.nbRow - 1)) {
      for (j <- 0 to (this.nbCol - 1)) {
        f((i, j), this (i, j))
      }
    }
  }

  def toDouble: MatrixLight[Double] = {
    val newMat = new MatrixLight[Double](this.nbRow, this.nbCol)
    newMat.data = scala.collection.mutable.Map() ++ data.mapValues((value: U) => numeric.toDouble(value))
    newMat
  }

  def toInt: MatrixLight[Int] = {
    val newMat = new MatrixLight[Int](this.nbRow, this.nbCol)
    newMat.data = scala.collection.mutable.Map() ++ data.mapValues((value: U) => numeric.toInt(value))
    newMat
  }

  // Doest work on matrices with negative values
  def normalizePositiveMatrix: MatrixLight[Double] = {
    val newMat = new MatrixLight[Double](this.nbRow, this.nbCol)
    val valmax = this.max
    val valmin = this.min
    data.foreach {
      case ((i, j), value) =>
        newMat.assign(i, j, (numeric.toDouble(value) - numeric.toDouble(valmin)) / numeric.toDouble(valmax))
    }
    newMat
  }

  // Doest work on matrices with negative values
  def proportional: MatrixLight[Double] = {
    val newMat = new MatrixLight[Double](this.nbRow, this.nbCol)
    val sum =  this.sum
    val divider = if(numeric.equiv(sum,numeric.zero)) numeric.one else sum
    data.foreach {
      case ((i, j), value) =>
        newMat.assign(i, j, numeric.toDouble(value) / numeric.toDouble(divider))
    }
    this.denominator = divider
    newMat
  }


  def reverseProportional: MatrixLight[Int] = {
    val newMat = new MatrixLight[Int](this.nbRow, this.nbCol)
    data.foreach {
      case ((i, j), value) =>
        newMat.assign(i, j,   numeric.toInt(numeric.times(value,this.denominator)))
    }
    newMat
  }

  def getDenominator : Int = numeric.toInt(denominator)

  def minus(m: MatrixLight[U]): MatrixLight[U] = {
    val newMat = new MatrixLight[U](this.nbRow, this.nbCol)
    val indices = this.data.keys.toSet ++ m.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>
        newMat.assign(i, j, numeric.minus(this (i, j), m(i, j)))
    }
    newMat
  }

  def norm(n: Int): Double = {
    var out = numeric.zero
    data.foreach {
      case ((i, j), value) =>
        var power = numeric.one
        for (i <- 1 to n) power = numeric.times(power, value)
        out = numeric.plus(out, power)
    }
    scala.math.pow(numeric.toDouble(out), 1 / n.toDouble)
  }

  def eq(m: MatrixLight[U]): Boolean = {
    var b = true
    val indices = this.data.keys.toSet.union(m.data.keys.toSet)
    indices.foreach {
      case (i: Int, j: Int) =>
        b = b && numeric.equiv(this (i, j), m(i, j))
    }
    b
  }

  def meanFilter(): MatrixLight[U] = {
    val newMat = new MatrixLight[U](this.nbRow, this.nbCol)
    for (i <- 1 to (this.nbRow - 2)) {
      for (j <- 1 to (this.nbCol - 2)) {
        for (ki <- -1 to 1) {
          for (kj <- -1 to 1) {
            newMat.add(i, j, this (i + ki, j + kj))
          }
        }
        newMat.div(i, j, numeric.fromInt(9))
      }
    }
    newMat
  }

  def carbageCollector() : Unit = {

    data = data.filter{case (( i : Int , j : Int), value : U) => !numeric.equiv(value,numeric.zero)}

  }


  override def toString : String = data.toString()

} 
