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
import fr.cnrs.liris.privamov.core.model.{Event, Trace}
import fr.cnrs.liris.privamov.core.sparkle.{DataFrame, SparkleEnv}
import fr.cnrs.liris.privamov.core.clustering._
import com.google.common.geometry._
import java.io._

import org.joda.time.{DateTimeConstants, Duration}

import scala.collection._
import scala.util.Random


@Op(
  category = "Attack",
  help = "Re-identification attack based on the MMC matching")
class MMCReIdentOp extends Operator[MMCReIdentIn, MMCReIdentOut] with SparkleOperator {


  override def execute(in: MMCReIdentIn, ctx: OpContext): MMCReIdentOut = {

    val dstrain = read(in.train, env)
    val dstest = read(in.test, env)


    val mapPOisTrain = formPOIs(dstrain,in.minPts,in.diameter,in.duration)
    val mapPOisTest = if(in.attack == "gambs" )  formPOIs(dstest,in.minPts,in.diameter, in.duration) else  immutable.Map[String,Seq[Cluster]]()
// form MMC transition matrices
    val mapMMCTrain = formMMCMap(dstrain,mapPOisTrain)
    val mapMMCTest = if(in.attack == "gambs" ) formMMCMap(dstest,mapPOisTest) else  immutable.Map[String,(MatrixLight[Double],Array[Double])]()

    val (rate, userMatches) = if(in.attack == "gambs" ) reIdentAttackGambs(dstest.keys.size,mapPOisTrain,mapMMCTrain,mapPOisTest,mapMMCTest) else reIdentAttackTower(dstest.keys.size,mapPOisTrain,mapMMCTrain,dstest)
// compute stats

   var stats = mapPOisTrain.map{
     case (k : String ,seq : Seq[Cluster]) =>
       val test = mapPOisTest.getOrElse(k,default = Seq[Cluster]())
       (k,seq.size.toString + " - " + test.size.toString)
   }
    dstest.keys.foreach{k =>
      if( !stats.contains(k)) {
        val test = mapPOisTest.getOrElse(k,default = Seq[Cluster]())
        val str = "0 - " + test.size.toString
        stats += k -> str

      }
    }
val str =  "" + dstest.keys.size + " - " + dstrain.keys.size + " - " + dstrain.keys.toSet.intersect(mapPOisTrain.keySet).size + " - " + dstest.keys.toSet.intersect(mapPOisTest.keySet).size  + " - " + dstrain.keys.toSet.intersect(dstest.keys.toSet).intersect(mapPOisTrain.keySet).intersect(mapPOisTest.keySet).size
   var matches = userMatches
     dstrain.keys.union(dstest.keys).toSet.foreach { u : String =>
      if (!matches.contains(u)) matches += u -> "-"
    }
    //MMCReIdentOut(matches,stats ,str,rate)
    print("PIT-Attack user re-identified : ")
    println(matches.filter{ pair => pair._1==pair._2}.keySet)
    println(s"PIT-Attack rate : $rate")
    MMCReIdentOut(matches,rate)

  }
  def reIdentAttackGambs(nbUser : Int , mapPOisTrain : immutable.Map[String,Seq[Cluster]] ,mapMMCTrain :  immutable.Map[String,(MatrixLight[Double],Array[Double])],  mapPOisTest : immutable.Map[String,Seq[Cluster]],mapMMCTest :  immutable.Map[String,(MatrixLight[Double],Array[Double])]) : (Double, immutable.Map[String, String]) = {

    var matches: scala.collection.immutable.Map[String, String] = scala.collection.immutable.Map[String, String]()
    var nbMatches: Int = 0

    mapPOisTest.par.foreach{
      case( k : String , states_k : Seq[Cluster]) =>
        var order = immutable.Map[String, Double]()
        mapPOisTrain.foreach{
          case( u : String , states_u : Seq[Cluster]) =>
            val dist = d(states_k,mapMMCTest(k),states_u,mapMMCTrain(u))
            order += (u -> dist)
        }
        val seq = order.toSeq.sortBy(_._2)
        synchronized(matches += (k -> seq.head._1))
        if (k == seq.head._1) nbMatches += 1
    }

    val rate: Double = nbMatches.toDouble / nbUser.toDouble
    (rate, matches)
  }

  def reIdentAttackTower(nbUser : Int , mapPOisTrain : immutable.Map[String,Seq[Cluster]] ,mapMMCTrain :  immutable.Map[String,(MatrixLight[Double],Array[Double])],  dstest : DataFrame[Trace]) : (Double, immutable.Map[String, String]) = {

    var matches: scala.collection.immutable.Map[String, String] = scala.collection.immutable.Map[String, String]()
    var nbMatches: Int = 0

    dstest.foreach{
     t =>
       val k = t.user
        //  println(s" user $k")
        var order = immutable.Map[String, Double]()
        mapPOisTrain.foreach{
          case( u : String , states_u : Seq[Cluster]) =>
            val dist = 1 - mmcCompatibility(t,states_u,mapMMCTrain(u))
            order += (u -> dist)
        }
        val seq = order.toSeq.sortBy(_._2)
        synchronized(matches += (k -> seq.head._1))
        if (k == seq.head._1) nbMatches += 1
    }

    val rate: Double = nbMatches.toDouble / nbUser.toDouble
    (rate, matches)
  }


  def mmcCompatibility( t : Trace, states : Seq[Cluster] , mmc :  (MatrixLight[Double],Array[Double]) ) : Double = {
   var log = 0.0
    val events = t.events
    val labels = Array.fill[Int](events.size)(-1)
    var prev = -1
    var lastPoi = -1
    for(i <- events.indices){
      val e = events(i)
      var dist = new Distance(200)
      for(j <- states.indices){
        val state = states(j)
        if(state.centroid.distance(e.point) < dist  )
          {
            labels(i) = j
            dist = state.centroid.distance(e.point)
          }
      }
      val dest = labels(i)
      if(lastPoi== -1) {
        lastPoi = dest
      }else {
        // transition  prev -> dest
        if (prev != dest  && dest != -1) {
          log = log + math.log10(mmc._1(lastPoi, dest))
        }

      }
      prev = dest
    }
    -log
  }

  def d( states_k : Seq[Cluster], mmc_k : (MatrixLight[Double],Array[Double])  , states_u : Seq[Cluster] , mmc_u :  (MatrixLight[Double],Array[Double]) ) : Double = {
   val stat = stationary_distance(states_k,mmc_k,states_u,mmc_u)
   val prox = proximity_distance(states_k,mmc_k,states_u,mmc_u)

   if(prox < 100000 && stat > 2000 ) prox else stat

  }

  def stationary_distance(states_k : Seq[Cluster], mmc_k : (MatrixLight[Double],Array[Double])  , states_u : Seq[Cluster] , mmc_u :  (MatrixLight[Double],Array[Double]) ) : Double = {
var dist = 0.0
    for(i <- states_k.indices){
      val pi = states_k(i)
      var min_distance = new Distance(1E8)
      for(j <- states_u.indices){
        val pj = states_u(j)
        val currentDistance = pi.centroid.distance(pj.centroid)
        if(currentDistance < min_distance) min_distance = currentDistance
      }
      dist = dist + min_distance.meters*mmc_k._2(i)
    }
    dist
  }


  def sym_stationary_distance(states_k : Seq[Cluster], mmc_k : (MatrixLight[Double],Array[Double])  , states_u : Seq[Cluster] , mmc_u :  (MatrixLight[Double],Array[Double]) ) : Double = {
    val dku = stationary_distance(states_k , mmc_k   , states_u , mmc_u  )
val duk = stationary_distance(states_u , mmc_u   , states_k , mmc_k  )
 dku + duk / 2.0
  }

  def proximity_distance(states_k : Seq[Cluster], mmc_k : (MatrixLight[Double],Array[Double])  , states_u : Seq[Cluster] , mmc_u :  (MatrixLight[Double],Array[Double]) ) : Double = {
  val delta = new Distance(100)
    var rank = 10
   val  v1 = mmc_k._2
    val  v2 = mmc_u._2
  val vo1 = v1.zipWithIndex.sortBy( _._1)
    val vo2 = v2.zipWithIndex.sortBy( _._1)
    var score = 0


    //for(i <- 0 to  math.min(states_k.size,states_u.size)){
    for(i <-states_k.indices.intersect(states_u.indices)){
      val pk =  states_k(vo1(i)._2)
      val pu =  states_u(vo2(i)._2)
      if(pk.centroid.distance(pu.centroid) < delta) {
        score += rank
      }
      rank = rank/2
      if(rank  == 0 ) rank = 1

    }
    val dist = if(score> 0) 1.0/score.toDouble else 100000
dist
  }



    def formPOIs(ds: DataFrame[Trace],minPts : Int , epsilon : Distance, duration : Duration ) : immutable.Map[String,Seq[Cluster]] = {
    var mmcMapPOisTrain = immutable.Map[String,Seq[Cluster]]()
  val clusterMachine = new PoisClusterer(duration,epsilon, minPts)
  ds.foreach{
     t =>
       val poiSet = clusterMachine.clusterKeepCluster(t.events)
       if(poiSet.nonEmpty) synchronized(mmcMapPOisTrain +=  (t.user -> poiSet))
   }
   mmcMapPOisTrain
}

  def formMMCMap(ds: DataFrame[Trace], mapPOis : immutable.Map[String,Seq[Cluster]] ) : immutable.Map[String,(MatrixLight[Double],Array[Double])] = {
    var mapTransMat = immutable.Map[String,(MatrixLight[Double],Array[Double])]()
    ds.foreach{
      t =>
        val poiset = mapPOis.get(t.user)
        poiset match {
          case Some(pois) => {
            if(pois.nonEmpty)
            synchronized(mapTransMat +=  (t.user ->  formTransitionMatrix(t.events,pois)))
          }
          case _ =>
        }

    }
    mapTransMat
  }

  def formTransitionMatrix(events : Seq[Event], states : Seq [Cluster]) : (MatrixLight[Double],Array[Double]) = {
    val labels = Array.fill[Int](events.size)(-1)
    val mat = new MatrixLight[Int](states.size,states.size)
    val count = Array.fill[Int](states.size)(0)
    var prev = -1
    var lastPoi = -1
    for(i <- events.indices){
      val e = events(i)
       for(j <- states.indices){
            val state = states(j)
            if(state.events.contains(e)) labels(i) = j
       }
      val dest = labels(i)
      if(dest!= -1) count(dest) = count(dest) +1
      if(lastPoi== -1) {
        lastPoi = dest
      }else {
        // transition  prev -> dest
        if (prev != dest  && dest != -1) {
          mat.inc(lastPoi, dest)
        }

      }
      prev = dest
    }
    val s = count.sum.toDouble
    val vector : Array[Double] = count.map(c => c.toDouble/s)
    (mat.proportional,vector)
  }





  def printMatrice[U](mat: Map[String, MatrixLight[U]]): Unit = {
    mat.foreach { m =>
      println(s"user : ${m._1} - Nb_(i,j) = ${m._2.data.keys.size}")
      println("-------------------------")
      if (m._2.nbCol > 1000 || m._2.nbRow > 1000) m._2.printNoneEmptyCells() else m._2.printMatrix()
      println("-------------------------")

    }
  }





}

case class MMCReIdentIn(
                         @Arg(help = "Input train dataset")
                          train: Dataset,
                         @Arg(help = "Input test dataset")
                          test: Dataset,
                         @Arg(help = "Clustering parameter : minimum points in a cluster")
                          minPts: Int = 1,
                         @Arg(help = "Clustering parameter : maximum size cluster")
                          diameter: Distance = new Distance(200),
                         @Arg(help = "Clustering parameter : maximum cluster duration")
                          duration : Duration = new Duration(3600),
                         @Arg(help = "Attack")
                            attack : String = "gambs"
                        )

case class MMCReIdentOut(
                           @Arg(help = "Matches between users") matches: immutable.Map[String, String],
                         //  @Arg(help = "POI stats") stats: immutable.Map[String, String],
                          // @Arg(help = "POI missing") missPOi: String,
                           @Arg(help = "Re-Ident rate") rate: Double
                         )


