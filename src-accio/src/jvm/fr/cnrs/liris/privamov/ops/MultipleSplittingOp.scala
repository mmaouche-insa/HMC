package fr.cnrs.liris.privamov.ops


import com.github.nscala_time.time.Imports._
import com.google.inject.Inject
import fr.cnrs.liris.accio.core.api._
import fr.cnrs.liris.privamov.core.model.Event
import fr.cnrs.liris.privamov.core.sparkle.SparkleEnv


@Op(
  category = "metric",
  help = "Re-identification attack using POIs a the discriminating information.")
class MultipleSplittingOp extends Operator[MultipleSplittingIn, MultipleSplittingOut]  with SlidingSplitting with SparkleOperator {

  override def execute(in: MultipleSplittingIn, ctx: OpContext): MultipleSplittingOut = {

    val data = read(in.data, env)

    val split = (buffer: Seq[Event], curr: Event) => (buffer.head.time to curr.time).duration >= in.delta
    val out = data.flatMap(t => transform(t,split))

   // out.foreach(t => println(s"user = ${t.user}\t id=${t.id}\t l= ${t.duration.toStandardHours}"))
    MultipleSplittingOut(write(out,ctx.workDir))

  }


}


case class MultipleSplittingIn(
                               @Arg(help = "Cutting interval after each time")
                               delta : org.joda.time.Duration,
                               @Arg(help = "Dataset to cut")
                               data: Dataset)


case class MultipleSplittingOut(
                                 @Arg(help = "Dataset to cut")
                                 data: Dataset)