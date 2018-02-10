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
import fr.cnrs.liris.common.geo.Distance
import fr.cnrs.liris.privamov.core.clustering.DTClusterer
import fr.cnrs.liris.privamov.core.model.{Poi, PoiSet, Trace}
import fr.cnrs.liris.privamov.core.sparkle.{DataFrame, SparkleEnv}

import com.google.common.geometry._


@Op(
  category = "metric",
  help = "Re-identification attack using POIs a the discriminating information.")
class ReIdentMatricesOp extends Operator[ReIdentMatricesIn, ReIdentMatricesOut] with SparkleOperator {

  override def execute(in: ReIdentMatricesIn, ctx: OpContext): ReIdentMatricesOut = {
    val trainDs = read(in.train, env)
    val testDs = read(in.test, env)
    /*
    //val trainClusterer = new DTClusterer(in.duration, in.diameter)
    //val testClusterer = new DTClusterer(in.testDuration.getOrElse(in.duration), in.testDiameter.getOrElse(in.diameter))
    //val trainPois = getPois(trainDs, trainClusterer)
    //val testPois = getPois(testDs, testClusterer)

    val distances = getDistances(trainPois, testPois)
    val matches = getMatches(distances)
  */
    //val matches = getMatches(distances)
    //val successRate = getSuccessRate(trainPois, matches)

   ReIdentMatricesOut()
  }
/*
  private def getCellsIntensity(ds : DataFrame[Trace]) = {

  ds.map(tr => tr.events.toArray
  )
  ds.map { tr =>
    val id = tr.id
    val events = tr.events
    println(s"the id = $id")
    events.map{ e =>
      val cellId = S2CellId.fromPoint(new S2Point(e.point.x,e.point.y,0.0))



    }

  }



}
*/

  /*private def getDistances() = {
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

  private def getMatches(distances: Map[String, Seq[(String, Double)]]) = {
    distances.map { case (testUser, res) =>
      if (res.isEmpty) {
        testUser -> "-"
      } else {
        testUser -> res.head._1
      }
    }
  }

  private def getSuccessRate(trainPois: Seq[PoiSet], matches: Map[String, String]) = {
    val trainUsers = trainPois.map(_.user)
    matches.map { case (testUser, trainUser) => if (testUser == trainUser) 1 else 0 }.sum / trainUsers.size.toDouble
  }*/
}

case class ReIdentMatricesIn(
                               @Arg(help = "Train dataset")
                               train: Dataset,
                               @Arg(help = "Test dataset")
                               test: Dataset)

case class ReIdentMatricesOut(

                             //   @Arg(help = "Matches between users from test and train datasets")
                              //  matches: Map[String, String],
                              //  @Arg(help = "Correct re-identifications rate")
                              //  rate: Double
)