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
class MatriceGenOp extends Operator[MatriceGenIn, MatriceGenOut] with SparkleOperator {
  override def execute(in: MatriceGenIn, ctx: OpContext): MatriceGenOut = {
    // read the data

    val ds = read(in.data, env)
    val nbUsers = ds.keys.length
    val dimensions = computeMatricesSize(in.lat, in.lng, in.diag, in.cellSize)
    var trainOutputMap: Map[String, Array[Array[Int]]] = Map[String, Array[Array[Int]]]().empty
    // Array.fill[Array[Int]](dimensions._1)(Array.fill[Int](dimensions._2)(0))
    var testOutputMap: Map[String, Array[Array[Int]]] = Map[String, Array[Array[Int]]]().empty
    for (key <- ds.keys) {
      trainOutputMap += (key -> Array.fill[Array[Int]](dimensions._1 + 1)(Array.fill[Int](dimensions._2 + 1)(0)))
      testOutputMap += (key -> Array.fill[Array[Int]](dimensions._1 + 1)(Array.fill[Int](dimensions._2 + 1)(0)))
    }
   // println(s"DIMENSIONS = $dimensions")
  ds.foreach {
      t =>
        val l = t.events.length
        if (l != 0) {
          //println(s"LENGHT = $l")
          t.events.last.time
          val start_trace = t.events.head.time
          val duration = t.duration
          val durationTrain = t.duration.minus(t.duration.dividedBy(in.divider))
          val timeTestStart = start_trace.plus(durationTrain)
          val user = t.user
          val events = t.events
          events.foreach { e =>
            val p = e.point
            val j = math.floor((p.x - dimensions._3.x) / in.cellSize.meters).toInt
            val i = math.floor((p.y - dimensions._3.y) / in.cellSize.meters).toInt
        //println(s"($i,$j)")
          //  println(s"p = $p ")
            if (e.time.isBefore(timeTestStart)) {

              val mat = trainOutputMap(user)
              mat(i)(j) = mat(i)(j) + 1
              trainOutputMap += (user -> mat)
            }
            else {
              val mat = testOutputMap(user)
              mat(i)(j) = mat(i)(j) + 1
              testOutputMap += (user -> mat)

            }
          }
        } else {
          trainOutputMap += (t.user -> Array.fill[Array[Int]](dimensions._1 + 1)(Array.fill[Int](dimensions._2 + 1)(0)))
          testOutputMap += (t.user -> Array.fill[Array[Int]](dimensions._1 + 1)(Array.fill[Int](dimensions._2 + 1)(0)))
        }
    }





    val output = reIdent(trainOutputMap, testOutputMap, 2)

    MatriceGenOut(output._1)
  }


  def reIdent(trainMats: Map[String, Array[Array[Int]]], testMats: Map[String, Array[Array[Int]]], n: Int): Tuple2[Double,Map[String, String]] = {
    var matches: Map[String, String] = Map[String, String]()
    var nbMatches: Int = 0
    testMats.foreach {
      case (k: String, mat_k: Array[Array[Int]]) =>


        var order = Map[String, Double]()
        trainMats.foreach {
          case (u: String, mat_u: Array[Array[Int]]) =>
           order += (u -> d(normalizeMat(mat_k), normalizeMat(mat_u), n))
        }

        val seq = order.toSeq.sortBy(_._2)
       // println(seq)
        matches += (k -> seq.head._1)
        if (k == seq.head._1) nbMatches += 1

    }

    val rate: Double = nbMatches.toDouble / testMats.keys.size.toDouble
    (rate , matches )
  }

  def normalizeMat(mat: Array[Array[Int]]): Array[Array[Double]] = {

    val newMat = Array.fill[Array[Double]](mat.length)(Array.fill[Double](mat(0).length)(0))
    var valmax = Int.MaxValue
    var valmin = Int.MinValue
    for (i <- mat.indices) {
      for (j <- mat(i).indices) {
        valmax = math.max(valmax, mat(i)(j))
        valmin = math.min(valmin, mat(i)(j))
      }
    }
    for (i <- mat.indices) {
      for (j <- mat(i).indices) {
        newMat(i)(j) = (mat(i)(j) - valmin).toDouble / (valmax - valmin).toDouble
      }
    }
    meanFilter(newMat)

  }

  def matEqual(m1: Array[Array[Double]], m2: Array[Array[Double]]): Boolean = {
    var b = true
    for (i <- m1.indices) {
      for (j <- m1(i).indices) {
        b = b && (m1(i)(j) == m2(i)(j))
      }
    }
    b
  }

  def meanFilter(mat: Array[Array[Double]]): Array[Array[Double]] = {
    val newMat = Array.fill[Array[Double]](mat.length)(Array.fill[Double](mat(0).length)(0))
    for (i <- 1 to mat.length-2) {
      for (j <- 1 to mat(i).length -2 ) {
          for(ki <- -1 to 1 ){
            for(kj <- -1 to 1 ){
              newMat(i)(j) = newMat(i)(j) + mat(i+ki)(j+kj)
            }
          }
        newMat(i)(j) = newMat(i)(j)/9
      }
    }
    newMat
  }


  def d(m1: Array[Array[Int]], m2: Array[Array[Int]], n: Int): Double = {
    var d: Double = 0

    for (i <- m1.indices) {
      for (j <- m1(i).indices) {
        d += math.pow(m1(i)(j) - m2(i)(j), n)
      }
    }

    d = math.pow(d, 1.0 / n)

    //println(s"d= $d")
    d
  }
  def d(unkownMat: Array[Array[Double]], knownMat : Array[Array[Double]], k: Int, x : Int): Double = {
    var d: Double =0.0
  /*  var noneEmptyCells : Seq[(Int,Int)] = Seq[(Int,Int)]()
    for (i <- unkownMat.indices) {
      for (j <- unkownMat(i).indices) {
       if(unkownMat(i)(j)> 1E-10) noneEmptyCells = noneEmptyCells ++  (i->j)
      }
    }
    var noneEmptyCellsArray : Array[(Int,Int)] = noneEmptyCells.toArray()
    var tmpK = 0 ;
    var tab = Array.fill[Double](k)(0.0)
    while(tmpK < k ){

    }*/
    d
  }
  def d(m1: Array[Array[Double]], m2: Array[Array[Double]], n: Int): Double = {
    var d: Double = 0
    if (n > 0) {
      for (i <- m1.indices) {
        for (j <- m1(i).indices) {
          d += math.pow(m1(i)(j) - m2(i)(j), n)
        }
      }

      d = math.pow(d, 1.0 / n)

    } else {
      n match {
        case -1 => d = dconv2(m1, m2)/math.sqrt(dconv2(m1, m1)*dconv2(m2, m2))
        case -2 => d = dKL(m2,m1) // not symmetric
      }

    }
    d
  }

  def dconv2(m1: Array[Array[Double]], m2: Array[Array[Double]]): Double = {
    var dc = 0.0
    var d_i_1 = Array.fill[Double](m1.length)(0)
    var d_i_2 = Array.fill[Double](m2.length)(0)
    var d_j_1 = Array.fill[Double](m1(0).length)(0)
    var d_j_2 = Array.fill[Double](m2(0).length)(0)
    var d1 = 0.0
    var d2 = 0.0
    for (i <- m1.indices) {
      for (j <- m1(0).indices) {
        d_i_1(i) = d_i_1(i) + m1(i)(j)
        d_i_2(i) = d_i_2(i) + m2(i)(j)
        d_j_1(j) = d_j_1(j) + m1(i)(j)
        d_j_2(j) = d_j_2(j) + m2(i)(j)
        d1 = m1(i)(j)
        d2 = m2(i)(j)
      }
    }
    for (i <- m1.indices) {
      d_i_1(i) = d_i_1(i) / m1.length
      d_i_2(i) = d_i_2(i) / m2.length
    }
    for (j <- m1(0).indices) {
      d_j_1(j) = d_j_1(j) / m1(0).length
      d_j_2(j) = d_j_2(j) / m2(0).length
    }
    d1 = d1 / (m1.length * m1(0).length)
    d2 = d2 / (m2.length * m1(0).length)

    for (i <- m1.indices) {
      for (j <- m1(0).indices) {
        dc = dc + (m1(i)(j) - d_i_1(i) - d_j_1(j) - d1) * (m2(i)(j) - d_i_2(i) - d_j_2(j) - d2)
      }
    }
    dc = dc / (m1.length * m1(0).length)

    dc
  }

  def dKL(m1: Array[Array[Double]], m2: Array[Array[Double]]): Double = {
    var dc = 0.0
    for (i <- m1.indices) {
      for (j <- m1(i).indices) {
          dc = dc + m1(i)(j)* math.log(m1(i)(j)/m2(i)(j))
      }
    }

dc
  }


  def computeMatricesSize(lat1: Double, lng1: Double, diag : Distance , cellSize: Distance): Tuple3[Int, Int, Point] = {
    val p1  = LatLng.degrees(lat1,lng1).toPoint
    // compute high point
    val p2=p1.translate(S1Angle.degrees(45),diag)
//println(s"p1 = $p1 - p2 = $p2")
    /*val topCornerleft = LatLng.degrees(math.max(lat1, lat2), math.min(lng1, lng2)).toPoint
    val topCornerRight = LatLng.degrees(math.max(lat1, lat2), math.max(lng1, lng2)).toPoint
    val bottomCornerleft = LatLng.degrees(math.min(lat1, lat2), math.min(lng1, lng2)).toPoint
    val bottomCornerRight = LatLng.degrees(math.min(lat1, lat2), math.max(lng1, lng2)).toPoint
*/
    val topCornerleft = Point(math.min(p1.x, p2.x), math.max(p1.y, p2.y))
    val topCornerRight = Point(math.max(p1.x, p2.x), math.max(p1.y, p2.y))
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

  def printMatrice(mat: Map[String, Array[Array[Int]]]): Unit = {
    mat.foreach { m =>
      println(s"user : ${m._1}")
      println("-------------------------")
      //  m._2 foreach { row => row foreach print; println }
      m._2 foreach { row => row.foreach {
        e =>
          print(e)
          print("\t")
      }

        println
      }
      println("-------------------------")

    }
  }
}


case class MatriceGenIn(


                         @Arg(help = "Input dataset")
                         data: Dataset,
                         @Arg(help = "Proportion of the test set in the form 1/divider ")
                         divider: Int,
                         @Arg(help = "Lower point latitude")
                         lat: Double,
                         @Arg(help = "Lower point longitude")
                         lng: Double,
                         @Arg(help = "Diagonal distance")
                         diag : Distance,
                         @Arg(help = "Diagona angle (degrees)")
                         ang : Option[Double],
                         @Arg(help = "Higher point latitude (override diag & angle)")
                         lat2: Option[Double],
                         @Arg(help = "Higher point latitude (override diag & angle)")
                         lng2: Option[Double],
                         @Arg(help = "Cell Size in meters")
                         cellSize: Distance)


case class MatriceGenOut(
                          //  @Arg(help = "Output Train matrices") trainMats: Map[String,Array[Array[Int]]],
                          // @Arg(help = "Output Train matrices") testMats:   Map[String,Array[Array[Int]]]
                           @Arg(help = "Re-Ident rate") rate : Double
                        )

