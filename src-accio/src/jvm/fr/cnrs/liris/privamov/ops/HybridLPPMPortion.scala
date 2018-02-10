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
import fr.cnrs.liris.common.geo.{Distance, _}
import fr.cnrs.liris.privamov.core.model.{Event, Trace}
import fr.cnrs.liris.privamov.core.sparkle.{DataFrame, SparkleEnv}
import com.google.common.geometry._
import java.io._

import fr.cnrs.liris.common.util.StringUtils
import org.joda.time.DateTimeConstants

import scala.collection.{mutable, _}
import scala.util.Random


@Op(
  category = "LPPM",
  help = "Apply a Hybrid Version of GeoI Promesse")
class HybridLPPMPortionOp  extends Operator[HybridLPPMPortionIn, HybridLPPMPortionOut] with SparkleOperator {


  override def execute(in: HybridLPPMPortionIn, ctx: OpContext): HybridLPPMPortionOut = {

    // read the data
    val train = read(in.train, env)
    val nonObf = read(in.nonObf, env)
    val geoI = read(in.geoI, env)
    val promesse = read(in.promesse, env)

    val matMatchOp = new MatMatchingPortionOp

    
    val matMatchNonObfIn =  MatMatchingPortionIn(train=in.train,test=in.nonObf,cellSize = in.cellSize,lat1 = in.lat1, lng1=in.lng1,lat2=Option(in.lat2),lng2 = Option(in.lng2))
    val matMatchNonObfOut =  matMatchOp.execute(matMatchNonObfIn,ctx)
    val matchesNonObf = matMatchNonObfOut.matches

    val matMatchgeoIIn =  MatMatchingPortionIn(train=in.train,test=in.geoI,cellSize = in.cellSize,lat1 = in.lat1, lng1=in.lng1,lat2=Option(in.lat2),lng2 = Option(in.lng2))
    val matMatchgeoIOut =  matMatchOp.execute(matMatchgeoIIn,ctx)
    val matchesgeoI = matMatchgeoIOut.matches

    val matMatchPromesseIn =  MatMatchingPortionIn(train=in.train,test=in.promesse,cellSize = in.cellSize,lat1 = in.lat1, lng1=in.lng1,lat2=Option(in.lat2),lng2 = Option(in.lng2))
    val matMatchPromesseOut =  matMatchOp.execute(matMatchPromesseIn,ctx)
    val matchesPromesse = matMatchPromesseOut.matches


   val outputDataFrame =  nonObf.map{
      t =>
        val user = t.user
        val id = t.id

        val matchNonObf = matchesNonObf.getOrElse(id,default = "-")
        val matchgeoI = matchesgeoI.getOrElse(id,default = "-")
        val matchPromesse = matchesPromesse.getOrElse(id,default = "-")

        if(!StringUtils.compareUser(id,matchNonObf)) t
        else if(!StringUtils.compareUser(id,matchgeoI)) geoI.load(id).toSeq.head
        else if(!StringUtils.compareUser(id,matchPromesse)) promesse.load(id).toSeq.head
        else t
    }

    val outputDataset = write(outputDataFrame,ctx.workDir)

    val matMatchHybridOut = (new MatMatchingPortionOp).execute( MatMatchingPortionIn(train=in.train,test=outputDataset,cellSize = in.cellSize,lat1 = in.lat1, lng1=in.lng1,lat2=Option(in.lat2),lng2 = Option(in.lng2)),ctx)
    HybridLPPMPortionOut(matchesNonObf,matchesgeoI,matchesPromesse,outputDataset,matMatchNonObfOut.rate,matMatchgeoIOut.rate,matMatchPromesseOut.rate,matMatchHybridOut.rate)
  }
}



case class HybridLPPMPortionIn(
                            @Arg(help = "Training dataset")
                            train: Dataset,
                            @Arg(help = "non-obf dataset")
                            nonObf: Dataset,
                            @Arg(help = "GeoI dataset")
                            geoI: Dataset,
                            @Arg(help = "Promesse dataset")
                            promesse: Dataset,
                            @Arg(help = "Clustering maximum diameter")
                            diameter: Distance = Distance(200),
                            @Arg(help = "Clustering minimum duration")
                            duration: org.joda.time.Duration = new org.joda.time.Duration(3600),

                            @Arg(help = "Cell Size in meters")
                            cellSize: Distance = Distance(800),
                            @Arg(help = "Lower point latitude")
                            lat1: Double =  -61.0 ,
                            @Arg(help = "Lower point longitude")
                            lng1: Double = -131.0 ,
                            @Arg(help = "Higher point latitude (override diag & angle)")
                            lat2: Double = 80.0 ,
                            @Arg(help = "Higher point latitude (override diag & angle)")
                            lng2: Double = 171
                          )

case class HybridLPPMPortionOut(
                                   @Arg(help = "Matches between users non obfuscated data") matchesMatMatchNonObf: immutable.Map[String, String]
                                 , @Arg(help = "Matches between users geoI data") matchesMatMatchgeoI: immutable.Map[String, String]
                                 , @Arg(help = "Matches between users promesse data") matchesMatMatchPromesse: immutable.Map[String, String]
                                   ,@Arg(help = "Output dataset")  output: Dataset
                                  ,@Arg(help = "Rate non obf")
                                   rateNonObf: Double
                                   ,@Arg(help = "Rate GeoI")
                                   rateGeoI: Double
                                   ,@Arg(help = "Rate Promesse")
                                   ratePromesse: Double
                                ,@Arg(help = "Rate Hybrid")
                                    rateHybrid: Double
)


