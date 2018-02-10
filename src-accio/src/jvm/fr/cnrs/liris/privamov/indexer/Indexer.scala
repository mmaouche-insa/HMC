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

package fr.cnrs.liris.privamov.indexer

import java.util.UUID

import com.github.nscala_time.time.Imports._
import com.google.common.geometry.S2CellId
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.mappings.FieldType._
import com.typesafe.scalalogging.StrictLogging
import fr.cnrs.liris.privamov.core.sparkle.DataFrame
import fr.cnrs.liris.privamov.core.model.Trace
import fr.cnrs.liris.common.util.HashUtils
import org.elasticsearch.common.geo.GeoPoint

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

/**
 * Handles the indexation of traces into Elasticsearch clusters. For now, only the ingestion of
 * whole traces is supported (i.e., not real-time ingestion).
 *
 * For each event, the following fields are indexed.
 * - user (string)
 * - location (geo point)
 * - time (date)
 * - time_secsofday (integer): number of seconds since midnight
 * - time_dayofweek (integer): day of the week (1 = Monday to 7 = Sunday)
 * - time_dayofyear (integer): day of year
 * - country (string): country code (2 letters)
 * - zipcode (string): city country-specific zipcode
 * - cell_0 (string): token of the S2 cell at level 0
 * - ...
 * - cell_30 (string): token of the S2 cell at level 30
 *
 * @param elasticClient An Elasticsearch client
 * @param indexName     An index name
 */
class Indexer(elasticClient: ElasticClient, indexName: String) extends StrictLogging {
  /**
   * Index a list of traces.
   *
   * @param dataset  A dataset of traces
   * @param typ      A mapping type
   * @param timezone A timezone for the elements of the dataset
   */
  def run(dataset: DataFrame[Trace], typ: String, timezone: DateTimeZone): Unit = {
    // First step: ensure the index and mappings are created.
    val preparation = elasticClient.execute(indexExists(indexName))
        .map(_.isExists)
        .flatMap(maybeCreateIndex)
        .flatMap(_ => elasticClient.execute(getMapping(indexName / typ)))
        .map(_.mappings.nonEmpty)
        .flatMap(exists => maybeCreateMapping(exists, typ))
    Await.result(preparation, Duration.Inf)

    // Second step: actually index each trace.
    dataset.foreach(addToIndex(typ, _, timezone))
  }

  private def maybeCreateIndex(exists: Boolean) = {
    if (exists) {
      Future.successful(Unit)
    } else {
      elasticClient.execute(create.index(indexName)).andThen {
        case Success(_) => logger.info(s"Created index '$indexName'")
      }
    }
  }

  private def maybeCreateMapping(exists: Boolean, typ: String) = {
    if (exists) {
      // Elasticsearch cannot delete existing mappings, we assume that if a mapping for the given
      // type exists, then it is correct.
      Future.successful(Unit)
    } else {
      elasticClient.execute {
        val coreFields = Seq(
          field("user") typed StringType,
          field("location") typed GeoPointType,
          field("time") typed DateType,
          field("time_secsofday") typed IntegerType,
          field("time_dayofweek") typed IntegerType,
          field("time_dayofyear") typed IntegerType,
          field("time_year") typed IntegerType,
          field("country") typed StringType,
          field("zipcode") typed StringType)
        val cellFields = (0 to 30).map { level =>
          field(s"cell_$level") typed StringType
        }
        putMapping(indexName / typ).fields(coreFields ++ cellFields: _*)
      }.andThen {
        case Success(_) => logger.info(s"Created mapping '$indexName/$typ'")
      }
    }
  }

  private def addToIndex(typ: String, trace: Trace, timezone: DateTimeZone) = {
    val future = elasticClient.execute {
      bulk(trace.events.zipWithIndex.map { case (event, idx) =>
        val time = event.time.toDateTime(timezone)
        val uid = HashUtils.sha1(UUID.randomUUID().toString)
        val latLng = event.point.toLatLng
        val cell = S2CellId.fromLatLng(latLng.toS2)

        val coreFields = Seq(
          "user" -> event.user,
          "location" -> new GeoPoint(latLng.lat.degrees, latLng.lng.degrees),
          "time" -> time.toString,
          "time_secsofday" -> time.secondOfDay.get,
          "time_dayofweek" -> time.dayOfWeek.get,
          "time_dayofyear" -> time.dayOfYear.get,
          "time_year" -> time.year.get)
        val cellFields = (0 to 30).map { level =>
          s"cell_$level" -> cell.parent(level).toToken
        }
        index.into(indexName -> typ).fields(coreFields ++ cellFields: _*).id(uid)
      })
    }.andThen {
      case Success(_) => logger.info(s"Indexed trace of user ${trace.user} (${trace.size} events)")
      case Failure(e) => logger.warn(s"Exception while indexing trace of user ${trace.user}", e)
    }.recover { case _ => Unit }
    Await.result(future, Duration.Inf)
  }
}