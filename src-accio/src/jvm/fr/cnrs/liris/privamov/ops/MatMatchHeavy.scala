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
// VERSION BADLY CODED
package fr.cnrs.liris.privamov.ops

import com.google.inject.Inject
import fr.cnrs.liris.accio.core.api._
import fr.cnrs.liris.common.geo._
import fr.cnrs.liris.privamov.core.model.Trace
import fr.cnrs.liris.privamov.core.sparkle.{DataFrame, SparkleEnv}
import com.google.common.geometry._
import java.io._
import scala.util.Random


@Op(
  category = "Attack",
  help = "Re-identification attack based on the matrix matching")
class MatMatchHeavyOp  extends Operator[MatMatchHeavyIn, MatMatchHeavyOut] with SparkleOperator {


  override def execute(in: MatMatchHeavyIn, ctx: OpContext): MatMatchHeavyOut = {
    // read the data
    val ds = read(in.data, env)
  // rectangular point
    val (p1,p2) = initializePoint(in)
    val (rds,ratio) = restrictArea(ds,p1,p2)
    val dimensions = computeMatricesSize(p1,p2,in.cellSize)
    val (trainOutputMap,testOutputMap) = formSingleMatrices(rds,in.divider,dimensions,in.cellSize)


   // printMatrixCsvfile(trainOutputMap)

    val meantrain = emptyCellsRatio(trainOutputMap)
    println(s" mean zeroes =  $meantrain")
    val (rate,matches) = reIdent(trainOutputMap, testOutputMap, in.distanceType)

    MatMatchHeavyOut(matches,ratio,meantrain, rate)
  }

  def formSingleMatrices(ds : DataFrame[Trace], divider : Int , dimensions : (Int,Int,Point), cellSize : Distance): (Map[String, Array[Array[Int]]],Map[String, Array[Array[Int]]]) ={

    var trainOutputMap: Map[String, Array[Array[Int]]] = Map[String, Array[Array[Int]]]().empty
    var testOutputMap: Map[String, Array[Array[Int]]] = Map[String, Array[Array[Int]]]().empty
    for (key <- ds.keys) {
      trainOutputMap += (key -> Array.fill[Array[Int]](dimensions._1 + 1)(Array.fill[Int](dimensions._2 + 1)(0)))
      testOutputMap += (key -> Array.fill[Array[Int]](dimensions._1 + 1)(Array.fill[Int](dimensions._2 + 1)(0)))
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
    (trainOutputMap,testOutputMap)
  }
  def reIdent(trainMats: Map[String, Array[Array[Int]]], testMats: Map[String, Array[Array[Int]]], n: Int): Tuple2[Double, Map[String, String]] = {
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
        matches += (k -> seq.head._1)
        if (k == seq.head._1) nbMatches += 1
    }
    val rate: Double = nbMatches.toDouble / testMats.keys.size.toDouble

    (rate, matches)
  }

  def normalizeMat(mat: Array[Array[Int]]): Array[Array[Double]] = {

    val newMat = Array.fill[Array[Double]](mat.length)(Array.fill[Double](mat(0).length)(0))
    var valmax = Int.MinValue
    var valmin = Int.MaxValue
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
    newMat
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

  def dnbZero(unknownMat: Array[Array[Double]], knownMat: Array[Array[Double]]): Double = {
    var d: Double = 0.0
var nb1 = 0
var nb2 = 0
    for (i <- unknownMat.indices) {
      for (j <- unknownMat(i).indices) {

        if (unknownMat(i)(j) > 1E-10 ) {
              nb1+= 1
          if(knownMat(i)(j) > 1E-10) nb2+=1
        }

      }
    }
    d = nb2.toDouble / nb2.toDouble
    d
  }


  def dNoneDet(unknownMat: Array[Array[Double]], knownMat: Array[Array[Double]], k: Int, x: Int): Double = {
    var d: Double = 0.0
    val rnd = new Random()
    var noneEmptyCells: Seq[(Int, Int)] = Seq[(Int, Int)]()
    for (i <- unknownMat.indices) {
      for (j <- unknownMat(i).indices) {
        if (unknownMat(i)(j) > 1E-10) {
          noneEmptyCells = noneEmptyCells :+ (i -> j)
        }

      }
    }
    // all the non zeros indices
    val noneEmptyCellsArray: Array[(Int, Int)] = noneEmptyCells.toArray
    val k2 = Math.min(k,noneEmptyCellsArray.length)
    var tmpx = 0
    // repeat x times the process
    while (tmpx < x){
      // this iteration chosen cells
    var chosenCells: Seq[(Int, Int)] = Seq[(Int, Int)]()
    var tmpK = 0
    val mask = Array.fill[Boolean](noneEmptyCellsArray.length)(false)
    while (tmpK < k2) {

      val ind: Int = rnd.nextInt(noneEmptyCellsArray.length)
      println(s" tmpk/k =  $tmpK/$k2 ;  ind = $ind ; tmpx/x = $tmpx/$x ")
      if (!mask(ind)) {
        mask(ind) = true
        chosenCells = chosenCells :+ noneEmptyCellsArray(ind)
        tmpK += 1
      }
    }
      d = d + vectorDistance(chosenCells,unknownMat,knownMat)
      tmpx+=1
  }
    d = d/ x
    d
  }
  def vectorDistance(chosenCells: Seq[(Int, Int)] , unknownMat: Array[Array[Double]], knownMat: Array[Array[Double]] ): Double ={
    var d : Double = 0.0
    chosenCells.foreach{
      ind =>
        d += Math.pow(unknownMat(ind._1)(ind._2) - knownMat(ind._1)(ind._2),2)
    }
    d = math.sqrt(d)
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
        case -1 => d = dconv2(m1, m2) / math.sqrt(dconv2(m1, m1) * dconv2(m2, m2))
        case -2 => d = dKL(m2, m1) // not symmetric
        case  0 => d = dNoneDet(m1,m2,10,10)
        case -3 => d = dnbZero(m1,m2)
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
        dc = dc + m1(i)(j) * math.log(m1(i)(j) / m2(i)(j))
      }
    }

    dc
  }


  def computeMatricesSize(p1 : Point , p2 :Point, cellSize: Distance): Tuple3[Int, Int, Point] = {


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


  def printMatrixCsvfile(mat: Map[String, Array[Array[Int]]]): Unit = {

    mat.foreach { m =>
      val bw = new BufferedWriter(new FileWriter(new File(s"${m._1}.csv" )))
      val matrix = normalizeMat(m._2)
      var str = "i\\j"

      for(j <- matrix(0).indices) str += s" , $j"
      str+= "\n"
      bw.write(str)
      for(i <- matrix.indices){
        str = s"$i"

        for(j <- matrix(i).indices){
          str += s", ${matrix(i)(j)}"

        }
        str+= "\n"
        bw.write(str)
      }
      bw.close()
    }
  }

  def printMatrixCsvText(mat: Map[String, Array[Array[Int]]]): Unit = {


    mat.foreach { m =>
      println(s"user : ${m._1}")
      println("-------------------------")
      val matrix = normalizeMat(m._2)
      print("i\\j")
      for(j <- matrix(0).indices) print(s" , $j")
      println("")
      //  m._2 foreach { row => row foreach print; println }
      for(i <- matrix.indices){
        print(s"$i")
        for(j <- matrix(i).indices){
           print(s", ${matrix(i)(j)}")
        }
        println("")
      }
      println("-------------------------")

    }
  }
  def printMatrix(mat: Array[Array[Double]]): Unit = {


      println("-------------------------")

      mat foreach { row => row.foreach {
        e =>
          print(e)
          print("\t")
      }

        println
      }
      println("-------------------------")

  }

  def initializePoint(in: MatMatchHeavyIn): (Point,Point) ={
    var p1 = Point(0, 0)
    var p2 = Point(0, 0)
    in.lat2 -> in.lng2 match {
      case (Some(lt), Some(lg)) => {
        p1 = LatLng.degrees(in.lat1, in.lng1).toPoint
        p2 = LatLng.degrees(lt, lg).toPoint
      }
      case _ => {
        p1 = LatLng.degrees(in.lat1, in.lng1).toPoint

        p2 = p1.translate(S1Angle.degrees(in.ang), in.diag)
      }
    }
    p1 -> p2
  }
  def emptyCellsRatio(map : Map[String , Array[Array[Int]]]) : Double = {
    val tab: Array[Double] = Array.fill[Double](map.keys.size)(0.0)
    var ind: Int = 0
    map.foreach {
      case (k: String, mat: Array[Array[Int]]) =>
        var nbZeroes = 0L
        for (i <- mat.indices) {
          for (j <- mat(i).indices) {
            if (mat(i)(j) == 0 ) nbZeroes += 1
          }
        }
          tab(ind) = nbZeroes.toDouble / (mat.indices.size * mat(0).indices.size).toDouble
          ind = ind + 1
        }

        val mean = tab.sum / tab.length
        mean
    }

  def restrictArea(ds: DataFrame[Trace], p1 : Point , p2 : Point): (DataFrame[Trace], Double) = {
    // Prepare the restrictive box
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
    //println(s"ratio = $ratio, nbtaken = $nbTaken ,  nbtot = $nbTot  ,   outcount =  ${output.count().toDouble}  ,  ds count = ${ds.count().toDouble}")
    //ratio = output.count().toDouble/ds.count().toDouble
   (output , ratio )
  }

}

case class MatMatchHeavyIn(
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
                       distanceType: Int = 2 ,
                       @Arg(help = "Cell Size in meters")
                       cellSize: Distance,
                       @Arg(help = "Type of distance (see doc, default euclidean distance")
                       distType: Int = 2
                     )

case class MatMatchHeavyOut(
                        @Arg(help = "Matches between users") matches: Map[String, String],
                        @Arg(help = "Ratio points taken by the restriction area") ratioPointsTaken : Double,
                        @Arg(help = "Mean Ratio of empty cells in matrices") meanEmptyCells : Double,
                        @Arg(help = "Re-Ident rate") rate : Double
                      )

