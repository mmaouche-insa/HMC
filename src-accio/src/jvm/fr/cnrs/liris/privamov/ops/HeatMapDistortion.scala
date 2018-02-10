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
import fr.cnrs.liris.privamov.core.model.Trace
import fr.cnrs.liris.privamov.core.sparkle.{DataFrame, SparkleEnv}
import java.io._
import scala.collection._


@Op(
  category = "metric",
  help = "Computes the HeatMaps' distortions between two datasets")
class HeatMapDistortionOp  extends Operator[HeatMapDistortionIn, HeatMapDistortionOut] with SparkleOperator {


  override def execute(in: HeatMapDistortionIn, ctx: OpContext): HeatMapDistortionOut = {
    // read the data
    val dstrain = read(in.train, env)
    val dstest = read(in.test, env)
    // rectangular point
    val (p1, p2) = initializePoint(in)
    val (rdstrain, ratio) = restrictArea(dstrain, p1, p2)
    val (rdstest, ratio2) = restrictArea(dstest, p1, p2)
    // Dimension of the matrix
    val dimensions = computeMatricesSize(p1, p2, in.cellSize)
    // compute HeatMaps
    val train = formSingleMatrices(rdstrain, dimensions, in.cellSize)
    val test = formSingleMatrices(rdstest, dimensions, in.cellSize)
    // Compute distortions
    var dist =  getDistortions(train,test,in.distanceType)
    // Add none found user in Train
    dstrain.keys.union(dstest.keys).toSet.foreach { u : String =>
      if (!dist.contains(u)) dist += u -> Double.NaN
    }

    val distWithoutNaN = dist.filter{case (k,p) => p!=Double.NaN}
    val avgDist = distWithoutNaN.values.sum / dist.size.toDouble


    HeatMapDistortionOut(dist,avgDist)
  }


  def formSingleMatrices(ds: DataFrame[Trace], dimensions: (Int, Int, Point), cellSize: Distance): immutable.Map[String, MatrixLight[Int]] = {

    var outputMap: scala.collection.immutable.Map[String, MatrixLight[Int]] = scala.collection.immutable.Map[String, MatrixLight[Int]]().empty
    ds.foreach{ t =>
      synchronized(outputMap += (t.user -> new MatrixLight[Int](dimensions._1, dimensions._2)))
      //println(t.user)
    }
    // println(s"before  ${ds.keys.size}")
    ds.foreach {
      t =>
        val l = t.events.length
        //     println(s"user ${t.id}  - l =  $l")

        if (l != 0) {
          // println(s"LENGHT = $l")
          t.events.last.time
          val start_trace = t.events.head.time
          val user = t.user
          val events = t.events
          events.foreach { e =>
            val p = e.point
            val j = math.floor((p.x - dimensions._3.x) / cellSize.meters).toInt
            val i = math.floor((p.y - dimensions._3.y) / cellSize.meters).toInt
            val mat = outputMap(user)
            mat.inc(i, j)
            synchronized(outputMap += (user -> mat))
          }
        }else{
          synchronized(outputMap  -= t.user)
        }
    }
    // println(s"after  ${outputMap.keys.size}")

    outputMap
  }

  def getDistortions(trainMats: immutable.Map[String, MatrixLight[Int]], testMats: immutable.Map[String, MatrixLight[Int]], n: Int):  immutable.Map[String, Double] = {
    testMats.par.map{
      case ( k , mat_k) =>
        if(!trainMats.contains(k)) k -> Double.NaN
        else{
          val mat_u = trainMats(k)
          k -> DistanceUtils.d(mat_k.proportional,mat_u.proportional,n)
        }
    }.seq
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


  def printMatrixCsvfile[U: Numeric](mat: immutable.Map[String, MatrixLight[U]]): Unit = {

    mat.foreach { m =>
      val bw = new BufferedWriter(new FileWriter(new File(s"${m._1}.csv")))
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


  def initializePoint(in: HeatMapDistortionIn): (Point, Point) = LatLng.degrees(in.lat1, in.lng1).toPoint -> LatLng.degrees(in.lat2, in.lng2).toPoint




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
    (output, ratio)
  }

}

case class HeatMapDistortionIn(
                          @Arg(help = "Input train dataset")
                          train: Dataset,
                          @Arg(help = "Input test dataset")
                          test: Dataset,
                          @Arg(help = "Type of distance metrics between matrices")
                          distanceType: Int = -51,
                          @Arg(help = "Cell Size in meters")
                          cellSize: Distance,
                          @Arg(help = "Lower point latitude")
                          lat1: Double =  -61.0 ,
                          @Arg(help = "Lower point longitude")
                          lng1: Double = -131.0 ,
                          @Arg(help = "Higher point latitude ")
                          lat2: Double = 80 ,
                          @Arg(help = "Higher point latitude ")
                          lng2: Double = 171
                        )

case class HeatMapDistortionOut(
                           @Arg(help = "Distortions (\"-\" = missing user in train or test) ") distortions : immutable.Map[String, Double],
                           @Arg(help = "Average distortion") avgDist: Double
                         )




