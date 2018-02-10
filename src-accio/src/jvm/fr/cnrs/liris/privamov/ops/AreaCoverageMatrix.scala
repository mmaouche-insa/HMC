/*
 * Copyright LIRIS-CNRS (2017)
 * Contributors: Mohamed Maouche  <mohamed.maouchet@liris.cnrs.fr>
 *
 * This software is a computer program whose purpose is to study location privacy.
 *
 * This software is governed by the CeCILL-B license under French law and
 * abiding by the rules of distribution of free software. You can use,
 * modify and/ or redistribute the software under the terms of the CeCILL-B
 * license as circulated by CEA, CNRS and INRIA at the following URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty and the software's author, the holder of the
 * economic rights, and the successive licensors have only limited liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading, using, modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean that it is complicated to manipulate, and that also
 * therefore means that it is reserved for developers and experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systems and/or
 * data to be ensured and, more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL-B license and that you accept its terms.
 */
package fr.cnrs.liris.privamov.ops

import com.google.common.geometry.S2CellId
import com.google.inject.Inject
import fr.cnrs.liris.accio.core.api._
import fr.cnrs.liris.common.geo._
import fr.cnrs.liris.common.util.Requirements._
import fr.cnrs.liris.privamov.core.model.Trace
import fr.cnrs.liris.privamov.core.sparkle.{DataFrame, SparkleEnv}

import scala.collection.immutable

@Op(
  category = "metric",
  help = "Compute area coverage difference between two datasets of traces")
class AreaCoverageMatrixOp  extends Operator[AreaCoverageMatrixIn, AreaCoverageMatrixOut] with SparkleOperator {

  override def execute(in: AreaCoverageMatrixIn, ctx: OpContext): AreaCoverageMatrixOut = {
    val  dstrain = read(in.train, env)
    val dstest = read(in.test, env)
    val dimensions = computeMatricesSize( LatLng.degrees(-61, -131).toPoint , LatLng.degrees(80,171).toPoint , in.cellSize)
    val (p1,p2) =  (LatLng.degrees(in.lat1,in.lng1).toPoint , LatLng.degrees(in.lat2,in.lng2).toPoint)
    val (rdstrain, ratio1) = restrictArea(dstrain, p1, p2)
    val (rdstest, ratio2) = restrictArea(dstest, p1, p2)
    val train = formSingleMatrices(rdstrain, dimensions, in.cellSize)
    val test = formSingleMatrices(rdstest, dimensions, in.cellSize)
    val stats = test.par.flatMap{case (k,mat_k)=>
      if(train.contains(k)) Some(areaCoverage((k,mat_k), (k,train(k))))
      else None
    }.seq
    val writtenStats = stats.map{case(k,p) => k -> ("" + p._1.toString + "="+ p._2.toString + "="+ p._3.toString) }.seq
    val fscores = stats.map{p => p._1 -> p._2._3}
    val avgACFscore = stats.values.map(p=>p._3).sum/stats.values.size
    val avgACrecall = stats.values.map(p=>p._2).sum/stats.values.size
    val avgACprecision = stats.values.map(p=>p._1).sum/stats.values.size
    AreaCoverageMatrixOut(writtenStats,fscores,avgACFscore,avgACrecall,avgACprecision)
  }

  private def areaCoverage(ref: (String,MatrixLight[Int]), res: (String,MatrixLight[Int])) = {
    val refCells = ref._2.indices
    val resCells = res._2.indices
    val matched = resCells.intersect(refCells).size
    (ref._1, (MetricUtils.precision(resCells.size, matched), MetricUtils.recall(refCells.size, matched), MetricUtils.fscore(refCells.size, resCells.size, matched)))
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
    (output, ratio)
  }


  def computeMatricesSize(p1 : Point  , p2 : Point  , cellSize: Distance): (Int, Int, Point) = {
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

  def formSingleMatrices(ds: DataFrame[Trace], dimensions: (Int, Int, Point), cellSize: Distance): immutable.Map[String, MatrixLight[Int]] = {

    var outputMap: scala.collection.immutable.Map[String, MatrixLight[Int]] = scala.collection.immutable.Map[String, MatrixLight[Int]]().empty
    ds.foreach{ t =>
      synchronized(outputMap += (t.user -> new MatrixLight[Int](dimensions._1, dimensions._2)))
      //println(t.user)
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
            mat.inc(i, j)
            outputMap += (user -> mat)
          }
        }else{
          outputMap  -= t.user
        }
    }
    outputMap
  }
  /*
  def statsAreaCoverage(trainMats: immutable.Map[String, MatrixLight[Int]], testMats: immutable.Map[String, MatrixLight[Int]], n: Int): (Double, immutable.Map[String, String]) = {
    // printMatrixCsvfile(testMats)


    var stats: immutable.Map[String, String] = immutable.Map[String, String]()
    var nbMatches: Int = 0

    testMats.par.foreach {
      case (k: String, mat_k: MatrixLight[Int]) =>
        var order = immutable.Map[String, Double]()
        var areaCovInfo = immutable.Map[String, String]()
        trainMats.foreach {
          case (u: String, mat_u: MatrixLight[Int]) =>
           val dist = DistanceUtils.d(mat_k.proportional, mat_u.proportional, n)
            val ac = areaCoverage((k,mat_k),(u,mat_u))
            order += (u -> dist)
            areaCovInfo += (u -> (ac._2._1.toString +"=" +ac._2._2.toString + "=" + ac._2._3.toString  ))
        }
         val seq = order.toSeq.sortBy(_._2)

        synchronized{
           seq.foreach{
             case (u,dist) =>
               val users = k+"_"+u
               val info = dist + "=" + areaCovInfo.getOrElse(u,"#")
               stats += ( users -> info )
           }
        }

        if (k == seq.head._1) nbMatches += 1
    }

    val rate: Double = nbMatches.toDouble / testMats.keys.size.toDouble
    (rate, stats)
  }
*/
}




case class AreaCoverageMatrixIn(
                           @Arg(help = "Train dataset") train: Dataset,
                           @Arg(help = "Test dataset") test: Dataset,
                           @Arg(help = "Cell Size in meters") cellSize: Distance,
                           //@Arg(help = "Type of distance metrics between matrices (default : Topsoe") distanceType: Int = -51

                           @Arg(help = "Lower point latitude")
                           lat1: Double =  -61.0 ,
                           @Arg(help = "Lower point longitude")
                           lng1: Double = -131 ,
                           @Arg(help = "Higher point latitude (override diag & angle)")
                           lat2: Double = 80,
                           @Arg(help = "Higher point latitude (override diag & angle)")
                           lng2: Double = 171
                               )

case class AreaCoverageMatrixOut(
                            @Arg(help = "Area coverage statistics") writtenStats: Map[String, String],
                            @Arg(help = "Area coverage statistics")  fscores : Map[String, Double],
                            @Arg(help = "Area coverage statistics")  avgFscore : Double,
                            @Arg(help = "Area coverage statistics")  avgrecall : Double,
                            @Arg(help = "Area coverage statistics")  avgprecision : Double

                                )
