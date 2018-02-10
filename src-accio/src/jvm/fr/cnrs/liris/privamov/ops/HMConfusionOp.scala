

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

import scala.util.control.Breaks._
import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.nio.file.{Files, Path}

import scala.util.Random
import com.google.common.geometry.S1Angle
import com.google.inject.Inject
import fr.cnrs.liris.accio.core.api._
import fr.cnrs.liris.common.geo._
import fr.cnrs.liris.privamov.core.model.{Event, Trace}
import fr.cnrs.liris.privamov.core.sparkle.{DataFrame, SparkleEnv}
import org.joda.time.{Duration, Instant}
import com.github.nscala_time.time.Imports._
import com.google.common.base.MoreObjects
import fr.cnrs.liris.privamov.core.clustering.DTClusterer
import fr.cnrs.liris.privamov.core.io.{CsvSink, TraceCodec}
import fr.cnrs.liris.privamov.core.lppm.SpeedSmoothing

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer
import scala.reflect.io.Path

@Op(
  category = "LPPM",
  help = "Transforms a dataset into a protected dataset using a knowledge of user past mobility")
class HMConfusionOp extends Operator[HMConfusionIn, HMConfusionOut] with SparkleOperator {

  override def execute(in: HMConfusionIn, ctx: OpContext): HMConfusionOut = {

    val dstrain = read(in.train, env)
    val dstest = read(in.test, env)


    val (p1, p2) = (LatLng.degrees(in.lat1, in.lng1).toPoint, LatLng.degrees(in.lat2, in.lng2).toPoint)
    val (rdstrain, ratio1) = restrictArea(dstrain, p1, p2)
    val (rdstest, ratio2) = restrictArea(dstest, p1, p2)

    val dimensions = computeMatricesSize(p1, p2, in.cellSize)

    val train = formSingleMatrices(rdstrain, dimensions, in.cellSize)
    val test = formSingleMatrices(rdstest, dimensions, in.cellSize)



    val nbIndicestrain = train.map(c => c._2.indices.size).sum
    val nbIndicestest = test.map(c => c._2.indices.size).sum

    val rnd = new Random(ctx.seed)
    val seeds = dstrain.keys.map(id => (id, rnd.nextLong())).toMap
    val output = test.par.map {       case (id, mat) =>       id -> transform(id, mat, train, -51, in,ctx,in.politic)     }.seq


    val newtest = output.map { case (id, out) => id -> out._1 }
    val newACFscore = output.map { case (id, out) => id -> out._2 }
    val worstACFscore = output.map { case (id, out) => id -> out._3 }
    val change = output.map { case (id, out) => id -> out._5 }
    val matches = output.map { case (id, out) => id -> out._6 }
    val statistics = output.map { case (id, out) => id -> ("" + out._2.toString + "=" + out._3.toString + "=" + out._4.toString + "=" + out._5.toString + "=" + out._6.toString) }
    val reIdentOut = reIdent(train,newtest,-51)
    val theoric_rate = reIdentOut._1
    val trainEventsMap = rdstrain.toArray.par.map(tr => tr.id -> traceMatrixCutting(tr, dimensions._3, in.cellSize)).seq.toMap

    val protectedDataset = rdstest.map { t =>
      if (newtest.contains(t.user)){
         createProtectedTraceAdd(t, newtest(t.user), trainEventsMap, dimensions._3, in.cellSize, seeds(t.id), in.minGap , in.fminGap.getOrElse(in.minGap),in.minTime1km, in.maxDuration)
      } else t
    }
   HMConfusionOut(statistics, newACFscore.values.sum / newACFscore.values.size, theoric_rate,matches.count( a => a._1==a._2).toDouble/matches.size, matches, write(protectedDataset, ctx.workDir))
  }



  override def isUnstable(in: HMConfusionIn): Boolean = true




 private  def createProtectedTraceRandom(tr: Trace, tprime: MatrixLight[Int], up: Point, cellSize: Distance, seed: Long) = {
    val eventsMap = traceMatrixCutting(tr, up, cellSize)

    val rnd = new Random(seed)
    val seedReinforce = rnd.nextLong()
    val seedAdd = rnd.nextLong()
    val reinforcedCellEvents = eventsMap.flatMap {
      p => constructSetEvent(p._2, tprime(p._1._1, p._1._2), seedReinforce)
    }
    val cellToAdd = tprime.indices.diff(eventsMap.keySet)

    val addedEvents = tprime.indices.diff(eventsMap.keySet).flatMap {
      ind =>
        fillCell(tr.user, ind, null, null, tprime(ind._1, ind._2), up, cellSize, seedAdd)

    }
    Trace(tr.id, reinforcedCellEvents ++ addedEvents)
  }

  private def createProtectedTraceReinforce(tr: Trace, tprime: MatrixLight[Int], up: Point, cellSize: Distance, seed: Long) = {
    val eventsMap = traceMatrixCutting(tr, up, cellSize)

    val rnd = new Random(seed)
    val seedReinforce = rnd.nextLong()
    val seedAdd = rnd.nextLong()
    val reinforcedCellEvents = eventsMap.flatMap {
      case p if tprime(p._1._1, p._1._2) != 0 =>  constructSetEvent(p._2, tprime(p._1._1, p._1._2), seedReinforce)
      case _ => None
    }
    Trace(tr.id, reinforcedCellEvents.toSeq.sortBy(e => e.time))
  }

  private def createProtectedTraceAdd(tr: Trace, tprime: MatrixLight[Int], trainEventsMap: Map[String, Map[(Int, Int), Seq[Event]]], up: Point, cellSize: Distance ,seed: Long, dtmin: Duration,fdtmin : Duration , tvmin: Duration,maxDuration : Duration) = {
    val eventsMap = traceMatrixCutting(tr, up, cellSize)
    var gs = gaps(tr, dtmin)

    //println(s"U${tr.id} GAPS-VS-CELLS for user ${tr.id} size =  ${gs.size} VS (REINF,ADD) (${eventsMap.size},${tprime.indices.diff(eventsMap.keySet).size}) ")

    val rnd = new Random(seed)
    val seedReinforce = rnd.nextLong()
    val seedAdd = rnd.nextLong()

    val reinforcedCellEvents = eventsMap.flatMap {
      case p if tprime(p._1._1, p._1._2) != 0 =>  constructSetEvent(p._2, tprime(p._1._1, p._1._2), seedReinforce).sortBy(e => e.time)
      case _ => None
    }


    val addedEvents = tprime.indices.diff(eventsMap.keySet).flatMap {
      case p if tprime(p._1, p._2) != 0 =>
        val out = fillCellWithOthers(trainEventsMap, gs, tr.id, p, tprime(p._1, p._2), up, cellSize,dtmin, fdtmin, tvmin,maxDuration, seedAdd)
        gs = trunkGap(gs,out._1,out._2,dtmin)
        out._2
      case _ => None
    }

    val allEvents = (reinforcedCellEvents++addedEvents).toSeq.sortBy(e => e.time)

    Trace(tr.id, erasePOIs(allEvents))
  }


 private def erasePOIs(events : Seq[Event]) = {
    val diameter = Distance.meters(200)
    val duration = Duration.standardMinutes(20)
    val clusterer = new DTClusterer(duration,diameter)
    val clusters = clusterer.cluster(events)

    events.filter(e => !clusters.exists(c => c.events.contains(e)))

  }

  private def erasePOIsWithSpeedSmoothing(events : Seq[Event]) = {
    val diameter = Distance.meters(200)
    val duration = Duration.standardMinutes(20)
    val clusterer = new DTClusterer(duration,diameter)
    val clusters = clusterer.cluster(events)
    val smoothEvents = clusters.flatMap(c => new SpeedSmoothing(Distance.meters(100)).transform(c.events.toSeq.sortBy(e => e.time)) )
    (events.filter(e => !clusters.exists(c => c.events.contains(e))) ++ smoothEvents).sortBy(e => e.time)

  }


  private def trunkGap(gaps: Map[Int, (Event, Event)],ind : Int , events : Seq[Event],dtmin : Duration) = {
  if(ind == -1) gaps
    else {
    require(events.nonEmpty)
    val gapToTrunk = gaps(ind)
    val trunkedGap = (events.last, gapToTrunk._2)
    val eraseOld = gaps - ind
    if (trunkedGap._1.time.plus(dtmin).isBefore(trunkedGap._2.time)) eraseOld + (ind -> trunkedGap)
    else eraseOld
    }
  }

  private def fillCellWithOthers(train: Map[String, Map[(Int, Int), Seq[Event]]], gaps: Map[Int, (Event, Event)], id: String, ind: (Int, Int), n: Int, up: Point, cellSize: Distance, dtmin: Duration,fdtmin : Duration, tvmin: Duration,maxDuration : Duration ,seed: Long) = {
    var fgaps = filterGaps(gaps, fdtmin, tvmin, ind, up, cellSize)
    var possible = ListBuffer[Seq[Event]]()
    for (v <- train) {
      if (v._2.getOrElse(ind, default = Seq[Event]()).nonEmpty) {
        val setOfEvents = cutEvents(v._2(ind),dtmin, events => events.nonEmpty)
        synchronized(possible ++= setOfEvents)
      }
    }
    val out = eventToFit(id, n, fgaps, possible, dtmin, seed,maxDuration)
    out

  }

  private def eventToFit(id: String, n: Int, fgaps: Map[Int, (Event, Event)], possible: ListBuffer[Seq[Event]], dtmin: Duration, seed: Long,maxDuration : Duration, method: String = "BEST-FIT") = {
    if (fgaps.nonEmpty && possible.nonEmpty) {
      var min: Option[Distance] = None
      var g = fgaps.head._2
      var es = possible.head
      var i = fgaps.head._1
      method match {
        case "BEST-DISTANCE" =>
          for ((j, gap) <- fgaps; events <- possible) {
            if (fits(gap, events)) {
              val d = distanceGapEvents(gap, events)
              min match {
                case Some(dmin) if d < dmin => min = Some(d); g = gap; i = j; es = events
                case None => min = Some(d)
                case _ =>
              }
            }
          }

        case "BEST-FIT" =>
          for ((j, gap) <- fgaps; events <- possible) {
            val cutEvents = cutToFit(events, gap,maxDuration)
            if (cutEvents.nonEmpty) {
              val d = distanceGapEvents(gap, events)
              min match {
                case Some(dmin) if d < dmin => min = Some(d); g = gap; i = j; es = events
                case None => min = Some(d)
                case _ =>
              }
            }
          }

      }
      (i, constructSetEvent(translateEventBetweenGap(id, g, es), n, seed), "FOUND")
    } else {
      (-1, Seq[Event](), if (fgaps.isEmpty) "NO-GAPS" else "NO-EVENTS")
    }
  }


  private def cutToFit(events: Seq[Event], gap: (Event, Event), maxDuration : Duration ) = {
    val gapDuration  = durationGap(gap)
    val delta =  if(gapDuration > maxDuration) gapDuration else maxDuration
    var  duration = Duration.millis(0)
    var sol = (-1, -1)
    var i = 0
    var j = 0
    while (i < events.size && j + 1 < events.size) {
      val dt = (events(i).time to events(j + 1).time).duration
      if (dt <= delta) {
        if (dt > duration) {
          sol = (i, j + 1)
          duration = dt
        }
        j = j + 1
      } else {
        i = i + 1
      }
    }
    if (sol._1 == -1) Seq[Event]()
    else events.slice(sol._1, sol._2 + 1)
  }

  private def distanceGapEvents(gap: (Event, Event), events: Seq[Event]) = gap._1.point.distance(events.head.point) + gap._2.point.distance(events.last.point)

  private def fillingPercentage(gap: (Event, Event), events: Seq[Event]) = durationGap(gap).getMillis.toDouble * 100 / durationEvents(events).getMillis.toDouble

  private def fits(gap: (Event, Event), events: Seq[Event]) = durationGap(gap) >= durationEvents(events)

  private def translateEventBetweenGap(id: String, gap: (Event, Event), events: Seq[Event]) = {
    val ta = gap._1.time
    val tb = gap._2.time
    val ts = events.head.time
    val te = events.last.time
    val initial_dt = new Duration(0)
    var instant = ta.plus(initial_dt)
    var output = ListBuffer[Event](Event(id, events.head.point, instant))
    for (i <- events.indices.tail) {
      val e1 = events(i - 1)
      val e2 = events(i)
      instant = instant.plus(durationGap((e1, e2)))
      output += Event(id, e2.point, instant)
    }
    output
  }

  private def cutEvents(events: Seq[Event], dtmin: Duration) = {
    var multiEvents = ListBuffer[Seq[Event]]()
    var buffer = ListBuffer[Event](events.head)
    for (i <- events.indices.tail) {
      val e1 = events(i - 1)
      val e2 = events(i)
      if ((e1.time to e2.time).duration > dtmin) {
        multiEvents += buffer
        buffer.clear()
      } else {
        buffer += e2
      }
    }
    multiEvents += buffer
    multiEvents
  }

  private def cutEvents(events: Seq[Event], dtmin : Duration , cond: (Iterable[Event] => Boolean)) = {
    var multiEvents = ListBuffer[Seq[Event]]()
    var buffer = ListBuffer[Event](events.head)
    for (i <- events.indices.tail) {
      val e1 = events(i - 1)
      val e2 = events(i)
      if ((e1.time to e2.time).duration > dtmin ) {
        if (cond(buffer)) multiEvents += buffer
        buffer.clear()
      }
      buffer += e2
    }
    if (buffer.nonEmpty && cond(buffer)) multiEvents += buffer
    multiEvents
  }


  private def durationEvents(events: Iterable[Event]) = (events.head.time to events.last.time).duration

  private def durationGap(gap: (Event, Event)) = (gap._1.time to gap._2.time).duration

  private def gaps(tr: Trace, dtmin: Duration) = {
    val events = tr.events
    var gaps = mutable.ListBuffer[(Event, Event)]()
    for (i <- events.indices.tail) {
      val a = events(i - 1)
      val b = events(i)
      if (a.time.plus(dtmin).isBefore(b.time)) {
        // If the duration between two events is greater then dt we have a new gap
        gaps += (a -> b)
      }
    }
    gaps.indices.zip(gaps).toMap
  }


  /**
    *
    * @param dtmin    : The minimal time between two events to form a gap
    * @param tvmin    : The maximal speed of a user (Time to go through 1000 meters)
    * @param ind      : Index of cell to filter with.
    * @param up       : Highest left point in the grid.
    * @param cellSize : Size of one square cell.
    *
    */
  private def filterGaps(gs: Map[Int, (Event, Event)], dtmin: Duration, tvmin: Duration, ind: (Int, Int), up: Point, cellSize: Distance) = {
    var fillterGaps = Map[Int, (Event, Event)]()

    for (i <- gs.keys) {
      val g = gs(i)
      val ta = g._1.time
      val tb = g._2.time
      val pa = g._1.point
      val pb = g._2.point
      val m = cellCenter(ind, up, cellSize)
      val dam = pa.distance(m)
      val dbm = pb.distance(m)

      val taprime = ta.plus(new Duration((tvmin.getMillis.toDouble * dam.kilometers).toLong))
      val tbprime = tb.minus(new Duration((tvmin.getMillis.toDouble * dbm.kilometers).toLong))
      if (taprime.plus(dtmin).isBefore(tbprime)) {
        fillterGaps += (i -> (Event(g._1.user, pa, taprime) -> Event(g._2.user, pb, tbprime)))
      }
    }
    fillterGaps
  }




  def fillCell(user: String, ind: (Int, Int), startCell: Seq[Event], endCell: Seq[Event], n: Int, up: Point, cellSize: Distance, seed: Long): Seq[Event] = {
    //val startEvent = startCell.last
    //val endEvent = endCell.head
    val rnd = new Random(seed)
    val (px, py) = cellSmallestPoint(ind, up, cellSize)
    (1 to n).map(k => Event(user, Point(px + rnd.nextDouble() * cellSize.meters, py + rnd.nextDouble() * cellSize.meters), new Instant()))

  }


  def cellSmallestPoint(ind: (Int, Int), up: Point, cellSize: Distance) = cellCornerLeftUp(ind, up, cellSize)

  def cellCornerLeftUp(ind: (Int, Int), up: Point, cellSize: Distance) = (up.x + ind._2 * cellSize.meters, up.y + ind._1 * cellSize.meters)

  def cellCornerLeftDown(ind: (Int, Int), up: Point, cellSize: Distance) = (up.x + ind._2 * cellSize.meters, up.y + (ind._1 + 1) * cellSize.meters)

  def cellCornerRightDown(ind: (Int, Int), up: Point, cellSize: Distance) = (up.x + (ind._2 + 1) * cellSize.meters, up.y + (ind._1 + 1) * cellSize.meters)

  def cellCornerRightUp(ind: (Int, Int), up: Point, cellSize: Distance) = (up.x + (ind._2 + 1) * cellSize.meters, up.y + ind._1 * cellSize.meters)

  def cellCenter(ind: (Int, Int), up: Point, cellSize: Distance) = {
    val (p1x, p1y) = cellCornerLeftUp(ind, up, cellSize)
    val (p2x, p2y) = cellCornerRightDown(ind, up, cellSize)
    Point((p1x + p2x) / 2, (p1y + p2y) / 2)
  }

  def distanceOfCells(c1 : (Int,Int), c2 : (Int,Int),up : Point , cellSize : Distance ) = cellCenter(c1,up,cellSize).distance(cellCenter(c2,up,cellSize))

  def tupleToPoint(coordinate: (Double, Double)) = Point(coordinate._1, coordinate._2)

  def isInCell(a: Point, ind: (Int, Int), up: Point, cellSize: Distance) = BoundingBox(tupleToPoint(cellCornerLeftUp(ind, up, cellSize)), tupleToPoint(cellCornerRightDown(ind, up, cellSize))).contains(a)


  def constructSetEvent(initialSet: Seq[Event], n: Int, seed: Long) = {
    var positions = initialSet
    while (positions.size < n) {
      positions = createDoublePosition(positions)
    }
    randomlyChoose(positions, n, seed)
  }


  def randomlyChoose(available: Seq[Event], n: Int, seed: Long) = {
    require(available.size >= n)
    var taken = Seq[Event]()
    val indices = available.indices.to[ListBuffer]
    val randomGenerator = new Random(seed)
    while (taken.size < n) {
      val rnd = randomGenerator.nextInt(indices.size)
      val ind = indices(rnd)
      val e = available(ind)
      indices.remove(rnd)
      taken = taken :+ e
    }
    taken
  }

  def createDoublePosition(in: Seq[Event]) = {
    var positions = Seq[Event]()
    val events =
      if (in.size == 1) {
        val e = in.head
        val p = e.point
        val newP = p.translate(S1Angle.degrees(30), new Distance(5))
        val newInstant = new Instant(e.time.getMillis + 5000)
        val eprime = Event(e.user, newP, newInstant)
        in :+ eprime
      } else in
    for (i <- events.indices.tail) {
      val e1 = events(i - 1)
      val e2 = events(i)
      val newP = new Point((e1.point.x + e2.point.x) / 2, (e1.point.y + e2.point.y) / 2)
      val i1 = e1.time
      val i2 = e2.time
      val newInstant = new Instant((i1.getMillis + i2.getMillis) / 2)
      positions = positions :+ e1
      positions = positions :+ Event(e1.user, newP, newInstant)
    }
    positions = positions :+ events.last

    positions
  }

  def traceMatrixCutting(tr: Trace, up: Point, cellSize: Distance) = {
    var output = immutable.Map[(Int, Int), Seq[Event]]()
    for (e <- tr.events) {
      val p = e.point
      val j = math.floor((p.x - up.x) / cellSize.meters).toInt
      val i = math.floor((p.y - up.y) / cellSize.meters).toInt
      val cellEvent = output.getOrElse((i, j), default = Seq[Event]())
      output += (i, j) -> (cellEvent :+ e)
    }
    output
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

  private def transform(user: String, mat_k: MatrixLight[Int], train: immutable.Map[String, MatrixLight[Int]], distanceType: Int, in: HMConfusionIn, ctx : OpContext,politic : String) = {

    val path = ctx.workDir.toAbsolutePath.toString+"iteration"
    new File(path).mkdir()
    val file = new File(path+"/"+user+".csv")
    val bw = new BufferedWriter(new FileWriter(file))
    val (matche, candidate) = bestMatch(user, mat_k, train, distanceType)
    if (matche != user) {
      bw.close()
      (mat_k, 1.0, 1.0, matche, false, matche)
    }
    else {
      val t = mat_k
      val u = train(matche)
      val v = train(candidate)
      val up = u.proportional
      val vp = v.proportional
      var it = 0
      var realit = 0
      val ITMAX = in.itmax
      var changed = false
      var tprime = t
      var best = v

      var stop = false
      var dtvOld = -1.0
      var dtuOld = -1.0

      while (it < ITMAX && !stop && realit < ITMAX*5) {
        val oldindices = tprime.indices
        tprime = oneStepTransformation(tprime, vp, up,politic)
        val newindices = tprime.indices
        val tp = tprime.proportional
        val dtu = DistanceUtils.d(tp, up, -51)
        val dtv = DistanceUtils.d(tp, vp, -51)
        val utbest = areaCoverageUnknown(t, best)._3
        val uttprime = areaCoverageUnknown(t, tprime)._3
        // println(s"---- $dtu > $dtv  &&  $uttprime > $utbest    ---  ${tprime.indices.size}  -- ${tprime.sum} --  ${tprime.data.count(p => p._2 != 0)}")
         //println(s"$user,$realit,$dtu,$dtv,$uttprime,$utbest,${tprime.indices.size},${tprime.sum}")
          bw.write(s"$user,$realit,$dtu,$dtv,$uttprime,$utbest,${tprime.indices.size},${tprime.sum}\n")
       if(dtvOld > 0 && dtuOld > 0 && dtu < dtv){
         if( dtv - dtvOld < 0 || dtu - dtuOld > 0) it = 0
       }
        dtvOld = dtv
        dtuOld = dtu
        if (dtu > dtv && uttprime > utbest) {
          best = tprime
          changed = true
          it = 0
          stop = true
        }
        it = it + 1
        realit = realit + 1
        if (utbest > 0.99 && dtu > dtv) stop = true
      }
      bw.close()
      val utbest = areaCoverageUnknown(t, best)._3
      val uttprime = areaCoverageUnknown(t, tprime)._3
      if (utbest > 1E-4) (best, areaCoverageUnknown(t, best)._3, areaCoverageUnknown(t, v)._3, candidate, changed, matche)
      else (mat_k, 1.0, 0.0, candidate, false, matche)
    }
  }

  private def oneStepTransformation(t: MatrixLight[Int], vp: MatrixLight[Double], up: MatrixLight[Double],politic : String): MatrixLight[Int] = {
    val nbPoints = t.sum
    val tp = t.proportional
    // points to remove
    // val tu = DistanceUtils.dotProduct(tp,up)
    //  val rm = DistanceUtils.dotProduct(tu,cv)
    // addPoints :  cT , cU , V
    //val consideredSetAdd = vp.indices.diff(t.indices.intersect(up.indices))
    val consideredSetAdd = vp.indices.diff(t.indices).diff(up.indices)
    val add = new MatrixLight[Double](t.nbRow, t.nbCol)
    //val setAdd = restrictToConnectedCells(t, consideredSetAdd.toSet)
    val setAdd =  consideredSetAdd
    setAdd.foreach { case (i, j) => add.assign(i, j, (1 - tp(i, j)) * (1 - up(i, j)) * vp(i, j)) }

    // Reinforce :  T , cU , V
    val consideredSetReInf = vp.indices.intersect(t.indices).diff(up.indices)
    val reinf = new MatrixLight[Double](t.nbRow, t.nbCol)
    consideredSetReInf.foreach { case (i, j) => reinf.assign(i, j, tp(i, j) * (1 - up(i, j)) * vp(i, j)) }

    // RM points :  T , U , cV
    val consideredSetRm= t.indices.intersect(up.indices).diff(vp.indices)
    val rm = new MatrixLight[Double](t.nbRow, t.nbCol)
    consideredSetRm.foreach { case (i, j) => rm.assign(i, j, (1-tp(i, j)) * up(i, j) * vp(i, j)) }
    val npts = math.max(nbPoints/500, 50)


  //  var tprime = addPoints(t, reinf, npts)
   val listOfTransformation = politic.split("-")
    var tprime = t
    if(listOfTransformation.contains("ADD")) tprime = addPoints(tprime, add, npts)
    if(listOfTransformation.contains("REINF")) tprime = addPoints(tprime, reinf, npts)
    if(listOfTransformation.contains("RM")) tprime = rmPoints(tprime,rm, npts )


    // var tprime = addPoints(t, add, npts)
     // tprime =  addPoints(tprime,reinf, npts )
    //tprime = rmPoints(tprime,rm, npts )

    val out = matrixToInt(tprime.proportional, nbPoints)
    out
  }

  private def restrictToConnectedCells(t: MatrixLight[Int], inCells: Set[(Int, Int)]) = {
    var noneEmptyCells = t.indices.toSet
    var outCells = Set[(Int, Int)]()
    var stop = false
    while (!stop) {
      stop = true
      inCells.foreach { p =>
        if (!noneEmptyCells.contains(p) && isConnected(noneEmptyCells, p)) {
          noneEmptyCells = noneEmptyCells + p
          outCells = outCells + p
          stop = false
        }
      }
    }
    outCells
  }

  private def isConnected(noneEmptyCells: Set[(Int, Int)], p: (Int, Int)): Boolean = {

    val s = Seq(-1, 0, 1).flatMap(x => Seq(-1, 0, 1).map(y => (x, y))).filter(x => x._1 != 0 || x._2 != 0)
    for (d <- s) {
      val (i, j) = (p._1 + d._1, p._2 + d._2)
      if (noneEmptyCells.contains((i, j))) return true
    }
    false
  }


  private def addPoints(t: MatrixLight[Int], add: MatrixLight[Double], n: Int): MatrixLight[Int] = {
    val sum = add.sum
    add.foreachNoneEmpty { case (ind: (Int, Int), add_value: Double) =>
      val newValue = math.max(0, t(ind._1, ind._2) + (add_value * n / sum).toInt)
      if (newValue > 0) t.assign(ind._1, ind._2, newValue)
      else t.remove(ind._1,ind._2)
    }
    t
  }

  private def rmPoints(t: MatrixLight[Int], rm: MatrixLight[Double], n: Int): MatrixLight[Int] = {
    val sum = rm.sum
    val out = rm.foreachNoneEmpty { case (ind: (Int, Int), rm_value: Double) =>
      val newValue = math.max(0, t(ind._1, ind._2)  - (rm_value * n / sum).toInt)
      if (newValue > 0) t.assign(ind._1, ind._2, newValue)
      else t.remove(ind._1,ind._2)
    }
    t
  }



  private def matrixToInt(m: MatrixLight[Double], multiplicator: Int): MatrixLight[Int] = {
    val newMat = new MatrixLight[Int](m.nbRow, m.nbCol)
    m.data.foreach {
      case ((i, j), value) =>
        newMat.assign(i, j, math.ceil(value * multiplicator).toInt)
    }
    newMat
  }

  private def bestMatch(user: String, mat_k: MatrixLight[Int], train: immutable.Map[String, MatrixLight[Int]], distanceType: Int): (String, String) = {
    var dists = immutable.Map[String, Double]()
    var fscores = immutable.Map[String, Double]()

    train.par.foreach {
      case (u, mat_u) =>
        synchronized {
          dists += u -> DistanceUtils.d(mat_k.proportional, mat_u.proportional, distanceType)
          fscores += u -> areaCoverage((user, mat_k), (u, mat_u))._2._3
        }
    }
    val distV = dists.toSeq.sortBy(_._2).head
    val tmp = fscores.toSeq.sortBy(_._2).reverse.head

    val fscoreV = if (tmp._1 == user) fscores.toSeq.sortBy(_._2).reverse.tail.head else tmp
    (distV._1, fscoreV._1)
  }

  private def areaCoverage(ref: (String, MatrixLight[Int]), res: (String, MatrixLight[Int])) = {
    val refCells = ref._2.indices
    val resCells = res._2.indices
    val matched = resCells.intersect(refCells).size
    (ref._1, (MetricUtils.precision(resCells.size, matched), MetricUtils.recall(refCells.size, matched), MetricUtils.fscore(refCells.size, resCells.size, matched)))
  }


  private def areaCoverageUnknown(ref: MatrixLight[Int], res: MatrixLight[Int]) = {
    val refCells = ref.indices
    val resCells = res.indices
    val matched = resCells.intersect(refCells).size
    (MetricUtils.precision(resCells.size, matched), MetricUtils.recall(refCells.size, matched), MetricUtils.fscore(refCells.size, resCells.size, matched))
  }


  def computeMatricesSize(p1: Point, p2: Point, cellSize: Distance): (Int, Int, Point) = {
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
    ds.foreach { t =>
      synchronized(outputMap += (t.user -> new MatrixLight[Int](dimensions._1, dimensions._2)))
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
            synchronized{outputMap += (user -> mat)}
          }
        }
    }
    outputMap
  }

  def statsAreaCoverage(trainMats: immutable.Map[String, MatrixLight[Int]], testMats: immutable.Map[String, MatrixLight[Int]], n: Int): (Double, immutable.Map[String, String]) = {
    var stats: immutable.Map[String, String] = immutable.Map[String, String]()
    var nbMatches: Int = 0

    testMats.par.foreach {
      case (k: String, mat_k: MatrixLight[Int]) =>
        var order = immutable.Map[String, Double]()
        var areaCovInfo = immutable.Map[String, String]()
        trainMats.foreach {
          case (u: String, mat_u: MatrixLight[Int]) =>
            val dist = DistanceUtils.d(mat_k.proportional, mat_u.proportional, n)
            val ac = areaCoverage((k, mat_k), (u, mat_u))
            order += (u -> dist)
            areaCovInfo += (u -> (ac._2._1.toString + "=" + ac._2._2.toString + "=" + ac._2._3.toString))
        }
        val seq = order.toSeq.sortBy(_._2)

        synchronized {
          seq.foreach {
            case (u, dist) =>
              val users = k + "_" + u
              val info = dist + "=" + areaCovInfo.getOrElse(u, "#")
              stats += (users -> info)
          }
        }

        if (k == seq.head._1) nbMatches += 1
    }

    val rate: Double = nbMatches.toDouble / testMats.keys.size.toDouble
    (rate, stats)
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  def reIdent(trainMats: immutable.Map[String, MatrixLight[Int]], testMats: immutable.Map[String, MatrixLight[Int]], n: Int): (Double, immutable.Map[String, String]) = {

    var matches: scala.collection.immutable.Map[String, String] = scala.collection.immutable.Map[String, String]()
    var nbMatches: Int = 0

    testMats.par.foreach {
      case (k: String, mat_k: MatrixLight[Int]) =>
        var order = immutable.Map[String, Double]()
        val l = trainMats.keys.size
        trainMats.foreach {
          case (u: String, mat_u: MatrixLight[Int]) =>
            val dist = DistanceUtils.d(mat_k.proportional, mat_u.proportional, n)
            order += (u -> dist)
        }
        val seq = order.toSeq.sortBy(_._2)
        synchronized(matches += (k -> seq.head._1))
        if (k == seq.head._1) nbMatches += 1
    }
    val rate: Double = nbMatches.toDouble / testMats.keys.size.toDouble
    (rate, matches)
  }
}


case class HMConfusionIn(
                          @Arg(help = "Train dataset") train: Dataset,
                          @Arg(help = "Test dataset") test: Dataset,
                          @Arg(help = "Cell Size in meters") cellSize: Distance,
                          @Arg(help = "Politics (ADD-REINF-RM or ADD-REINF ...)") politic: String = "ADD-REINF",
                          @Arg(help = "Type of distance metrics between matrices (default : Topsoe") distanceType: Int = -51,
                          @Arg(help = "Number of iteration without improvement") itmax: Int = 1000,
                          @Arg(help = "Gap minimum duration")
                          minGap: Duration = Duration.standardHours(1),
                          @Arg(help = "Gap minimum duration after accounting  the movement to the center of cell using tvmin")
                          fminGap: Option[Duration],
                          @Arg(help = "Acceptable minimum time to go through 1km")
                          minTime1km: Duration = Duration.standardSeconds(36),
                          @Arg(help = "Maximum duration of a set of events to fill a cell with")
                          maxDuration: Duration = Duration.standardHours(3),
                          @Arg(help = "Lower point latitude")
                          lat1: Double = -61.0,
                          @Arg(help = "Lower point longitude")
                          lng1: Double = -131,
                          @Arg(help = "Higher point latitude (override diag & angle)")
                          lat2: Double = 80,
                          @Arg(help = "Higher point latitude (override diag & angle)")
                          lng2: Double = 171
                        )

case class HMConfusionOut(
                           @Arg(help = "statistics  id -> Ut(T, new T ) , Minimum Utility , changed ") precision: Map[String, String],
                           @Arg(help = "Average F-score utility of dataset") avgAC: Double,
                           @Arg(help = "Theoretical rate before transformations") rateTheoritical : Double,
                           @Arg(help = "rate before transformations") rate : Double,
                           @Arg(help = "Matching re-identificaition") matches: Map[String, String],
                           @Arg(help = "Protected dataset") out: Dataset
                         )




