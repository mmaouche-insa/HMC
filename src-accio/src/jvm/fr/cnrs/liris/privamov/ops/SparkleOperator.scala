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


import java.nio.file.{Files, Path}

import fr.cnrs.liris.accio.core.api.{Dataset, OpContext, Operator}
import fr.cnrs.liris.common.util.Identified
import fr.cnrs.liris.privamov.core.io._
import fr.cnrs.liris.privamov.core.model.Trace
import fr.cnrs.liris.privamov.core.sparkle.{DataFrame, SparkleEnv}

import scala.reflect.{ClassTag, classTag}

trait SparkleOperator {
  this: Operator[_, _] =>

  // Create a Sparkle environment using the numProcs' flag to limit parallelism.
  // It is a poor-man's way to isolate execution in terms of CPU usage.
   val env = new SparkleEnv(math.max(1,sys.runtime.availableProcessors() - 1) )

  private[this] val encoders = Set(new StringCodec, new CsvEventCodec, new TraceCodec, new CsvPoiCodec, new CsvPoiSetCodec)
  private[this] val decoders = Set(new StringCodec, new CsvEventCodec, new TraceCodec, new CsvPoiCodec, new CsvPoiSetCodec)

  /**
    * Read a CSV dataset as a [[DataFrame]].
    *
    * @param dataset Dataset to read.
    * @tparam T Dataframe type.
    * @throws RuntimeException If there is no decoder to read as given type.
    */
  protected final def read[T: ClassTag](dataset: Dataset): DataFrame[T] = {
    val clazz = classTag[T].runtimeClass
    decoders.find(decoder => clazz.isAssignableFrom(decoder.elementClassTag.runtimeClass)) match {
      case None => throw new RuntimeException(s"No decoder available for ${clazz.getName}")
      case Some(decoder) => env.read(new CsvSource(dataset.uri, decoder.asInstanceOf[Decoder[T]]))
    }
  }
    /**
      * Read a CSV dataset as a [[DataFrame]].
      *
      * @param dataset Dataset to read.
      * @throws RuntimeException If there is no decoder to read as given type.
      */
    protected  def read(dataset: Dataset, env : SparkleEnv) = {
        require(dataset.format == "csv", s"Only CSV datasets are supported, got: ${dataset.format}")
     val src = new CsvSource(dataset.uri, new TraceCodec)
        env.read(src)
  }

  /**
    * Write a [[DataFrame]] as a CSV dataset, for a "data" output port.
    *
    * @param frame Dataframe to write.
    * @param ctx   Operator execution context.
    * @tparam T Dataframe type.
    * @throws RuntimeException If there is no encoder to write dataframe.
    */
  protected final def write[T <: Identified : ClassTag](frame: DataFrame[T], ctx: OpContext): Dataset =
    write[T](frame, ctx, "data")

  /**
    * Write a [[DataFrame]] as a CSV dataset, for a "data" output port.
    *
    * @param frame Dataframe to write.
    * @param workDir   Path of working.
    * @throws RuntimeException If there is no encoder to write dataframe.
    */
  protected final def write(frame: DataFrame[Trace], workDir : Path): Dataset =
    write(frame, workDir, "data")



  /**
    * Write a [[DataFrame]] as a CSV dataset.
    *
    * @param frame Dataframe to write.
    * @param ctx   Operator execution context.
    * @param port  Output port name.
    * @tparam T Dataframe type.
    * @throws RuntimeException If there is no encoder to write dataframe.
    */
  protected final def write[T <: Identified : ClassTag](frame: DataFrame[T], ctx: OpContext, port: String): Dataset = {
    val clazz = classTag[T].runtimeClass
    encoders.find(encoder => clazz.isAssignableFrom(encoder.elementClassTag.runtimeClass)) match {
      case None => throw new RuntimeException(s"No encoder available for ${clazz.getName}")
      case Some(encoder) =>
        val path = ctx.workDir.resolve(port).toAbsolutePath

        Files.createDirectories(path)
        frame.write(new CsvSink(path.toString, encoder.asInstanceOf[Encoder[T]]))
        Dataset(path.toString,"csv")
    }
  }

  /**
    * Write a [[DataFrame]] as a CSV dataset.
    *
    * @param frame Dataframe to write.
    * @param workDir   Path of working.
    * @param port  Output port name.
    * @throws RuntimeException If there is no encoder to write dataframe.
    */
  protected final def write(frame: DataFrame[Trace],workDir : Path , port: String): Dataset = {
        val path = workDir.resolve(port).toAbsolutePath
        Files.createDirectories(path)
        frame.write(new CsvSink(path.toString,new TraceCodec))
        Dataset(path.toString,"csv")
  }


}