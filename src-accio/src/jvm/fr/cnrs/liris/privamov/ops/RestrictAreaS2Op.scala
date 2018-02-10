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
class RestrictAreaS2Op  extends Operator[RestrictAreaS2In, RestrictAreaS2Out]  with SparkleOperator {
  override def execute(in: RestrictAreaS2In, ctx: OpContext): RestrictAreaS2Out = {
    // read the data
    val ds = read(in.data, env)
    // Prepare the region to restrict (using the S2 library)
    val region_rect = new S2LatLngRect(in.p1.toLatLng.toS2,in.p2.toLatLng.toS2)
    // Restrict the tracers to the region.
    val output =  ds.map { t =>
    t.filter{e =>
      region_rect.interiorContains(e.point.toLatLng.toS2)}
    }
    RestrictAreaS2Out(write(output, ctx.workDir))
  }
}







case class RestrictAreaS2In(


                              @Arg(help = "Input dataset")
                              data: Dataset,
                              @Arg(help = "First point")
                              p1: Point,
                              @Arg(help = "Second point")
                              p2: Point)



  case class RestrictAreaS2Out(
                              @Arg(help = "Output dataset") data: Dataset
                            )