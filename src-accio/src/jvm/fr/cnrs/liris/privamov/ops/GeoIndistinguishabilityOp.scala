/*
 * Copyright LIRIS-CNRS (2016)
 * Contributors: Vincent Primault <vincent.primault@liris.cnrs.fr>
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

import com.google.inject.Inject
import fr.cnrs.liris.accio.core.api._
import fr.cnrs.liris.privamov.core.lppm.Laplace
import fr.cnrs.liris.privamov.core.sparkle.SparkleEnv

import scala.util.Random

@Op(
  category = "lppm",
  help = "Enforce geo-indistinguishability guarantees on traces.",
  description = "Generate locations satisfying geo-indistinguishability properties. The method used here is the one " +
    "presented by the authors of the paper and consists in adding noise following a double-exponential distribution.")
class GeoIndistinguishabilityOp  extends Operator[GeoIndistinguishabilityIn, GeoIndistinguishabilityOut] with SparkleOperator {

  override def execute(in: GeoIndistinguishabilityIn, ctx: OpContext): GeoIndistinguishabilityOut = {
    val input = read(in.data, env)

    input.map( t =>  t.events)
    val rnd = new Random(ctx.seed)
    val seeds = input.keys.map(key => key -> rnd.nextLong()).toMap
    val output = input.map(trace => new Laplace(in.epsilon, seeds(trace.id)).transform(trace))
    GeoIndistinguishabilityOut(write(output, ctx.workDir))
  }

  override def isUnstable(in: GeoIndistinguishabilityIn): Boolean = true
}

case class GeoIndistinguishabilityIn(
  @Arg(help = "Privacy budget") epsilon: Double = 0.001,
  @Arg(help = "Input dataset") data: Dataset) {
  require(epsilon > 0, s"Epsilon must be strictly positive (got $epsilon)")
}

case class GeoIndistinguishabilityOut(
  @Arg(help = "Output dataset") data: Dataset)