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

import com.github.nscala_time.time.Imports._
import com.google.inject.Inject
import fr.cnrs.liris.accio.core.api._
import fr.cnrs.liris.privamov.core.model.Event
import fr.cnrs.liris.privamov.core.sparkle.SparkleEnv

@Op(
  category = "prepare",
  help = "Enforce a minimum duration between two consecutive events in traces.",
  description = "If the duration is less than a given threshold, events will be discarded until the next point " +
    "that fulfills the minimum duration requirement.")
class TemporalSamplingOp  extends Operator[TemporalSamplingIn, TemporalSamplingOut] with SlidingSampling with SparkleOperator {

  override def execute(in: TemporalSamplingIn, ctx: OpContext): TemporalSamplingOut = {
    val sample = (prev: Event, curr: Event) => (prev.time to curr.time).duration >= in.duration
    val output = read(in.data, env).map(transform(_, sample))
    val nbEvents = read(in.data, env).map(trace => trace.user -> trace.events.length)
    val nbEventsNew = output.map(trace => trace.user -> trace.events.length)

    val minOld = nbEvents.map( k => k._2).toArray.min
    val minNew = nbEventsNew.map( k => k._2).toArray.min
    val maxOld = nbEvents.map( k => k._2).toArray.max
    val maxNew = nbEventsNew.map( k => k._2).toArray.max
    val nbOld = nbEvents.map( k => k._2).toArray.sum
    val nbNew = nbEventsNew.map( k => k._2).toArray.sum
    val avgOld = nbOld.toDouble / nbEvents.keys.length.toDouble
    val avgNew = nbNew.toDouble / nbEventsNew.keys.length.toDouble
    val stdOld =  math.sqrt(nbEvents.map(k => math.pow(k._2.toDouble - avgOld,2)).toArray.sum / nbEvents.keys.length.toDouble)
    val stdNew =  math.sqrt(nbEventsNew.map(k => math.pow(k._2.toDouble - avgNew,2)).toArray.sum / nbEventsNew.keys.length.toDouble)
    TemporalSamplingOut(write(output, ctx.workDir),minOld,minNew,maxOld,maxNew,nbOld,nbNew,avgOld,avgNew,stdOld,stdNew)
  }
}

case class TemporalSamplingIn(
  @Arg(help = "Minimum duration between two consecutive events") duration: org.joda.time.Duration,
  @Arg(help = "Input dataset") data: Dataset)

case class TemporalSamplingOut(
  @Arg(help = "Output dataset") data: Dataset,
  @Arg(help = "Number of events of old dataset") minOld: Double,
  @Arg(help = "Number of events of new dataset") minNew: Double,
  @Arg(help = "Number of events of old dataset") maxOld: Double,
  @Arg(help = "Number of events of new dataset") maxNew: Double,
  @Arg(help = "Number of events of old dataset") sumOld: Double,
  @Arg(help = "Number of events of new dataset") sumNew: Double,
  @Arg(help = "Average number of events in old dataset") avgOld: Double,
  @Arg(help = "Average number of events in new dataset") avgNew: Double,
  @Arg(help = "Standard deviation of number of events by user in old dataset") stdOld: Double,
  @Arg(help = "Standard deviation of number of events by user in old dataset") stdNew: Double)