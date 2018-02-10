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

import com.github.nscala_time.time.Imports._
import com.google.inject.Guice
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import fr.cnrs.liris.common.flags.{Flag, FlagsModule, FlagsParserFactory}
import fr.cnrs.liris.privamov.core.io.{CabspottingSource, GeolifeSource}
import fr.cnrs.liris.privamov.core.sparkle.SparkleEnv
import org.elasticsearch.common.settings.Settings

case class IndexerFlags(
    @Flag(name = "type")
    typ: String,
    @Flag(name = "timezone")
    timezone: String = "Europe/Paris",
    @Flag(name = "elastic_uri")
    elasticUri: String = "localhost:9300")

object IndexerJobMain extends IndexerJob

class IndexerJob {
  def main(args: Array[String]): Unit = {
    val injector = Guice.createInjector(FlagsModule)
    val parserFactory = injector.getInstance(classOf[FlagsParserFactory])
    val flagsParser = parserFactory.create(allowResidue = true, classOf[IndexerFlags])
    flagsParser.parseAndExitUponError(args)
    val flags = flagsParser.as[IndexerFlags]

    val elasticClient = ElasticClient.transport(
      Settings.settingsBuilder.put("client.transport.ping_timeout", "30s").build,
      ElasticsearchClientUri(flags.elasticUri))

    val env = new SparkleEnv(1)
    val indexer = new Indexer(elasticClient, indexName = "event")
    try {
      flagsParser.residue.foreach { url =>
        val dataset = createDataset(env, flags.typ, url)
        indexer.run(dataset, flags.typ, DateTimeZone.forID(flags.timezone))
      }
    } finally {
      elasticClient.close()
      env.stop()
    }
  }

  private def createDataset(env: SparkleEnv, typ: String, url: String) = {
    val source = typ match {
      case "cabspotting" => CabspottingSource(url)
      case "geolife" => GeolifeSource(url)
      case _ => throw new IllegalArgumentException(s"Unknown type '$typ'")
    }
    env.read(source)
  }
}