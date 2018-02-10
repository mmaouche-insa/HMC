

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

package fr.cnrs.liris.privamov.ops


import fr.cnrs.liris.accio.core.api._
import fr.cnrs.liris.privamov.core.model.Trace
import fr.cnrs.liris.privamov.core.sparkle.DataFrame

import scala.io

@Op(
  category = "LPPM",
  help = "Transforms a dataset into a protected dataset using a knowledge of user past mobility")
class HybridLPPMUtilityOp extends Operator[HybridLPPMUtilityIn, HybridLPPMUtilityOut] with SparkleOperator {

  override def execute(in: HybridLPPMUtilityIn, ctx: OpContext): HybridLPPMUtilityOut = {
/*
    val non = read(in.non, env)
    val geoi = read(in.geoi, env)
    val promesse = read(in.promesse, env)
    val w4m = read(in.w4m, env)
    val hmc = read(in.hmc, env)
*/
    val data = Seq[DataFrame[Trace]](read(in.non, env) ,read(in.geoi, env) , read(in.promesse, env),read(in.w4m, env),read(in.hmc, env))//.map(_.filter(_.id.toDouble < 10.0))
    val matchesCount = extractMatchesCountByLPPM(in.matches,in.coefAtk,data.size)
    val lppmScoresMap = extractLPPMScore(matchesCount,in.utilityValues,in.coefUtility,nblppm = 5)
    val worstAtk = in.coefAtk.sum
   val output = data.head.map{
      t =>
        val id = t.id
        val bestLPPM = lppmScoresMap.getOrElse(id,default = Seq((worstAtk,0.0,4))).head._3
        data(bestLPPM).load(id).toSeq.head
    }

  val formatResult = lppmScoresMap.par.map{    p =>   (p._1 ,  ""+p._2.map(pp => pp.productIterator.mkString(",")).mkString(","))}.seq


    HybridLPPMUtilityOut(write(output, ctx.workDir),formatResult)

  }


  def extractLPPMScore(matchesCount : Map[String,Seq[Double]],pathUtility :String,coefUtility : Seq[Double],nblppm : Int ) = {
    val utilityMap = readCsvDouble(pathUtility)
    val nbUtility = coefUtility.size
   val lppmScoresMap = matchesCount.par.map {
     p =>
       val id = p._1
       val matchesCount = p._2
       //println(s"$matchesCount")
       val utilityValues = synchronized(utilityMap.getOrElse(id, default = Seq[Double](nblppm*nbUtility, Double.MaxValue)))
       val countWithUtility = matchesCount.zipWithIndex.map {
        case (mc,i) =>
          val lppmUtility = utilityValues.zipWithIndex.filter{ case (utilityValue, j) =>  i  == j % nblppm }.map(_._1)
          val agregateWithCoef = -lppmUtility.zip(coefUtility).map(v => v._1 * v._2).sum/coefUtility.sum
          (mc,agregateWithCoef,i)
       }.sorted

       (id,countWithUtility)
   }.seq
    lppmScoresMap
  }

  def extractMatchesCountByLPPM(path : String,coefAtk : Seq[Double] , nbLPPM : Int = 5) : Map[String,Seq[Double]]= {
val matchesMap = readCsv[String](path)
    val nbAtk = coefAtk.size

  val matchesCount = matchesMap.par.map{
    p =>
      val id = p._1
      val matches = p._2
      var v = 0.0
      var results = Seq[Double]()
      for((u,i) <- matches.zipWithIndex ){
        val atk = i % nbAtk
        v = v +  (if(id == u) 1.0 else 0.0)*coefAtk(atk)
        if(atk == nbAtk -1 ){
          results = results :+ v
          v = 0.0
        }
      }
      println(s"MATCH-COUNT-SIZE :  ${results.size}")
      println(s"MATCH-COUNT-RESULT:  ${results}")

      id -> results
  }.seq
    matchesCount
  }

  // Only appliable to  line =  id, v1,v2,v3 ...  and Header = ON
  def readCsvDouble(path : String) : Map[String,Seq[Double]] = {
    val bufferedSource = io.Source.fromFile(path)
    var matchesMap = Map[String,Seq[Double]]()
    for (line <- bufferedSource.getLines.drop(1)) {
      val cols = line.split(",").map(_.trim)
      matchesMap += cols.head -> cols.tail.map(_.toDouble).toSeq
    }
    matchesMap
  }

// Only appliable to  line =  id, v1,v2,v3 ...  and Header = ON
  def readCsv[T](path : String) : Map[String,Seq[T]] = {
    val bufferedSource = io.Source.fromFile(path)
    var matchesMap = Map[String,Seq[T]]()
    for (line <- bufferedSource.getLines.drop(1)) {
      val cols = line.split(",").map(_.trim)
      matchesMap += cols.head -> cols.tail.map(_.asInstanceOf[T]).toSeq
    }
matchesMap
  }

  def LPPM = Map(0 -> "non-obfuscated", 1 -> "Geo-I" , 2 -> "Promesse",3 -> "W4M", 4 -> "HMC")

  override def isUnstable(in: HybridLPPMUtilityIn): Boolean = true

}


case class HybridLPPMUtilityIn(
                          @Arg(help = "Path of the file containing the matches in the CSV format  id, matche for Atk1 on lppm1 ...., matches for Atk_k on lppm p") matches: String,
                          @Arg(help = "Path of the file containing the utility values in the CSV format id -> value for lppm1 , value for lppm2 ...") utilityValues: String,
                          //@Arg(help = "Re lunch the Attack (overrides the matches csv file") atkLunch: Boolean = false,
                          @Arg(help = "Coefficients of re-identification attacks  [AP-ATTACK, POI-ATTACK, PIT-Attack]") coefAtk : Seq[Double],
                          @Arg(help = "Coefficients of Utility Metrics  [Area Coverage, Spatial Distortion]") coefUtility : Seq[Double],
                          @Arg(help = "Data non-obfuscated") non: Dataset,
                          @Arg(help = "Data Geo-I") geoi: Dataset,
                          @Arg(help = "Data Promesse") promesse: Dataset,
                          @Arg(help = "Data W4M") w4m: Dataset,
                          @Arg(help = "Data HMC") hmc: Dataset)

case class HybridLPPMUtilityOut(
                                 @Arg(help = "Hybrid LPPM Result") data: Dataset,
                                 @Arg(help = "A Details Results of the computation") results: Map[String,String]
                         )




