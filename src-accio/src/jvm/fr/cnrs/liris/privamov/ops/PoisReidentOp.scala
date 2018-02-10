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

import java.io.File

import com.google.inject.Inject
import fr.cnrs.liris.accio.core.api._
import fr.cnrs.liris.common.geo.Distance
import fr.cnrs.liris.privamov.core.clustering.DTClusterer
import fr.cnrs.liris.privamov.core.model.{Poi, PoiSet, Trace}
import fr.cnrs.liris.privamov.core.sparkle.{DataFrame, SparkleEnv}

/**
  * Implementation of a re-identification attack using the points of interest as a discriminating information. The POIs
  * are used to model the behaviour of training users, and then extracted from the tracks of test users and compared to
  * those from the training users. The comparison here is done between set of POIs, and only the spatial information.
  *
  * Vincent Primault, Sonia Ben Mokhtar, Cédric Lauradoux and Lionel Brunie. Differentially Private
  * Location Privacy in Practice. In MOST'14.
  */
@Op(
  category = "metric",
  help = "Re-identification attack using POIs a the discriminating information.")
class PoisReidentOp  extends Operator[ReidentificationIn, ReidentificationOut] with SparkleOperator {

  override def execute(in: ReidentificationIn, ctx: OpContext): ReidentificationOut = {
    val trainDs = read(in.train, env)
    val testDs = read(in.test, env)
    val trainClusterer = new DTClusterer(in.duration, in.diameter)
    val testClusterer = new DTClusterer(in.testDuration.getOrElse(in.duration), in.testDiameter.getOrElse(in.diameter))


//    val trainPois = getPois(trainDs, trainClusterer)
 //   val testPois = getPois(testDs, testClusterer)


  /*  val trainPoisdata = getPOisDataFrame(trainDs, trainClusterer)
    val testPoisdata = getPOisDataFrame(testDs, testClusterer)


    val trainPois = getPOIsFromDataFrame(trainPoisdata)
    val testPois = getPOIsFromDataFrame(testPoisdata)
*/

  val trainPoiSet = getPOISetDataFrame(trainDs,trainClusterer)
    val testPoiSet = getPOISetDataFrame(testDs,testClusterer)

    val trainPois = trainPoiSet.toArray
    val testPois = testPoiSet.toArray

    val distances = getDistances(trainPois, testPois)
    var matches = getMatches(distances,trainDs.keys.intersect(testDs.keys).toSet)

   val successRate = getSuccessRateComplete(trainDs.keys.length, matches)

        trainDs.keys.union(testDs.keys).foreach(s => if(!matches.contains(s)) matches +=  s -> "-")
    //ReidentificationOut(distances.map { case (k, v) => k -> v.toMap }, matches, successRate)
    print("POI-Attack user re-identified : ")
    println(matches.filter{ pair => pair._1==pair._2}.keySet)
    println(s"POI-Attack rate : $successRate")

    val path = ctx.workDir.toAbsolutePath.toString+"iteration"
    new File(path).mkdir()
    write[PoiSet](trainPoiSet, ctx,"trainPOI")
    write[PoiSet](testPoiSet, ctx,"testPOI")

    ReidentificationOut(matches, successRate)

  }

  private def getPois(data: DataFrame[Trace], clusterer: DTClusterer) = {
    val allPois = data.flatMap(clusterer.cluster(_).map(c => Poi(c.events))).toArray
    allPois.groupBy(_.user).map { case (user, pois) =>  PoiSet(user, pois) }.toSeq

  }

  private def getPOisDataFrame(data: DataFrame[Trace], clusterer: DTClusterer) =  data.flatMap(clusterer.cluster(_).map(c => Poi(c.events)))

  private def getPOIsFromDataFrame(allPois : DataFrame[Poi]) = {
    allPois.toArray.groupBy(_.user).map { case (user, pois) => PoiSet(user, pois) }.toSeq

  }

  private def getPOISetDataFrame(data: DataFrame[Trace], clusterer: DTClusterer)= {
    data.map(t => PoiSet(t.user,clusterer.cluster(t).map(c => Poi(c.events))))
  }

  private def getDistances(trainPois: Seq[PoiSet], testPois: Seq[PoiSet]) = {
    val costs = collection.mutable.Map[String, Map[String, Double]]()
    testPois.foreach { pois =>
      val distances = if (pois.nonEmpty) {
        //Compute the distance between the set of pois from the test user and the models (from the training users).
        //This will give us an association between a training user and a distance. We only keep finite distances.
        trainPois.map(model => model.user -> model.distance(pois).meters).filter { case (u, d) => !d.isInfinite }.toMap
      } else {
        Map[String, Double]()
      }
      costs.synchronized {
        costs += pois.user -> distances
      }
    }
    costs.toSeq.map { case (user, model) =>
      user -> model.toSeq.sortBy(_._2).map { case (u: String, d: Double) => (u, d) }
    }.toMap
  }

  private def getMatches(distances: Map[String, Seq[(String, Double)]], userFull : Set[String]) = {
   var matches = distances.map { case (testUser, res) =>
      if (res.isEmpty) {
        testUser -> "-"
      } else {
        testUser -> res.head._1
      }
    }
    userFull.foreach { u =>
      if (!matches.contains(u)) matches += u -> "-"
    }
    matches
  }
  private def getSuccessRate(trainPois: Seq[PoiSet], matches: Map[String, String]) = {
    val trainUsers = trainPois.map(_.user)
    val nbMataches = matches.map { case (testUser, trainUser) =>
      if (testUser == trainUser) {
        1
      }
      else {
        0
      }
    }.sum
    val nbUsers = trainUsers.size.toDouble
    nbMataches.toDouble / nbUsers.toDouble
  }

  private def getSuccessRateComplete(nbUsers : Int, matches: Map[String, String]) = {
    val nbMataches = matches.map { case (testUser, trainUser) =>
      if (testUser == trainUser) {
        1
      }
      else {
        0
      }
    }.sum
    nbMataches.toDouble / nbUsers.toDouble
  }
}

case class ReidentificationIn(
                               @Arg(help = "Clustering maximum diameter")
                               diameter: Distance,
                               @Arg(help = "Clustering minimum duration")
                               duration: org.joda.time.Duration,
                               @Arg(help = "Override the clustering maximum diameter to use with the test dataset only")
                               testDiameter: Option[Distance],
                               @Arg(help = "Override the clustering minimum duration to use with the test dataset only")
                               testDuration: Option[org.joda.time.Duration],
                               @Arg(help = "Train dataset")
                               train: Dataset,
                               @Arg(help = "Test dataset")
                               test: Dataset)

case class ReidentificationOut(
                               // @Arg(help = "Distances between users from test and train datasets")
                                //distances: Map[String, Map[String, Double]],
                                @Arg(help = "Matches between users from test and train datasets")
                                matches: Map[String, String],
                                @Arg(help = "Correct re-identifications rate")
                                rate: Double)