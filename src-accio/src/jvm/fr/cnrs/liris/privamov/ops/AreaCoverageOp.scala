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

import com.google.common.geometry.S2CellId
import com.google.inject.Inject
import fr.cnrs.liris.accio.core.api._
import fr.cnrs.liris.common.util.Requirements._
import fr.cnrs.liris.privamov.core.model.Trace
import fr.cnrs.liris.privamov.core.sparkle.SparkleEnv

@Op(
  category = "metric",
  help = "Compute area coverage difference between two datasets of traces")
class AreaCoverageOp extends Operator[AreaCoverageIn, AreaCoverageOut] with SparkleOperator {

  override def execute(in: AreaCoverageIn, ctx: OpContext): AreaCoverageOut = {
    val train = read(in.train, env)
    val test = read(in.test, env)
    val metrics = train.zip(test).map { case (ref, res) => evaluate(ref, res, in.level) }.toArray
    val precision = metrics.map { case (k, v) => k -> v._1 }.toMap
    val recall = metrics.map { case (k, v) => k -> v._2 }.toMap
   val  fscore = metrics.map { case (k, v) => k -> v._3 }.toMap

    AreaCoverageOut(
     precision = precision,
      recall = recall ,
      fscore = fscore,
      avg_precision =  precision.values.sum / precision.values.size ,
      avg_recall =  recall.values.sum / recall.values.size ,
      avg_fscore =  fscore.values.sum / fscore.values.size
    )
  }

  private def evaluate(ref: Trace, res: Trace, level: Int) = {
    requireState(ref.id == res.id, s"Trace mismatch: ${ref.id} / ${res.id}")
    val refCells = getCells(ref, level)
    val resCells = getCells(res, level)
    val matched = resCells.intersect(refCells).size
    (ref.id, (MetricUtils.precision(resCells.size, matched), MetricUtils.recall(refCells.size, matched), MetricUtils.fscore(refCells.size, resCells.size, matched)))
  }

  private def getCells(trace: Trace, level: Int) =
    trace.events.map(rec => S2CellId.fromLatLng(rec.point.toLatLng.toS2).parent(level)).toSet
}

case class AreaCoverageIn(
  @Arg(help = "S2 cells levels") level: Int,
  @Arg(help = "Train dataset") train: Dataset,
  @Arg(help = "Test dataset") test: Dataset)

case class AreaCoverageOut(
  @Arg(help = "Area coverage precision") precision: Map[String, Double],
  @Arg(help = "Area coverage recall") recall: Map[String, Double],
  @Arg(help = "Area coverage F-score") fscore: Map[String, Double],
  @Arg(help = "Average area coverage precision") avg_precision: Double,
  @Arg(help = "Average area coverage recall") avg_recall: Double,
  @Arg(help = "Average area coverage F-score") avg_fscore: Double

                          )