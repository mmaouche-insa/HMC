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

import com.google.inject.TypeLiteral
import fr.cnrs.liris.accio.core.api.Operator
import net.codingwell.scalaguice.{ScalaModule, ScalaMultibinder}

/**
 * Guice module providing bindings for all operators defined in this package.
 */
object OpsModule extends ScalaModule {
  override def configure(): Unit = {
    val ops = ScalaMultibinder.newSetBinder(binder, new TypeLiteral[Class[_ <: Operator[_, _]]] {})
    ops.addBinding.toInstance(classOf[AreaCoverageOp])
    ops.addBinding.toInstance(classOf[BasicAnalyzerOp])
    ops.addBinding.toInstance(classOf[CollapseTemporalGapsOp])
    ops.addBinding.toInstance(classOf[CountQueriesDistortionOp])
    ops.addBinding.toInstance(classOf[DataCompletenessOp])
    ops.addBinding.toInstance(classOf[DurationSplittingOp])
    ops.addBinding.toInstance(classOf[EnforceDurationOp])
    ops.addBinding.toInstance(classOf[EnforceSizeOp])
    ops.addBinding.toInstance(classOf[EventSourceOp])
    ops.addBinding.toInstance(classOf[GaussianKernelSmoothingOp])
    ops.addBinding.toInstance(classOf[GeoIndistinguishabilityOp])
    ops.addBinding.toInstance(classOf[SizeSplittingOp])
    ops.addBinding.toInstance(classOf[SpatialGapSplittingOp])
    ops.addBinding.toInstance(classOf[PoisAnalyzerOp])
    ops.addBinding.toInstance(classOf[PoisRetrievalOp])
    ops.addBinding.toInstance(classOf[PromesseOp])
    ops.addBinding.toInstance(classOf[PoisReidentOp])
    ops.addBinding.toInstance(classOf[SequentialSplittingOp])
    ops.addBinding.toInstance(classOf[SpatialSamplingOp])
    ops.addBinding.toInstance(classOf[SpatialDistortionOp])
    ops.addBinding.toInstance(classOf[TemporalDistortionOp])
    ops.addBinding.toInstance(classOf[TemporalGapSplittingOp])
    ops.addBinding.toInstance(classOf[TransmissionDelayOp])
    ops.addBinding.toInstance(classOf[TemporalSamplingOp])
    ops.addBinding.toInstance(classOf[UniformSamplingOp])
    ops.addBinding.toInstance(classOf[Wait4MeOp])
    ops.addBinding.toInstance(classOf[RestrictAreaS2Op])
    ops.addBinding.toInstance(classOf[RestrictAreaOp])
    ops.addBinding.toInstance(classOf[MatriceGenOp])
    ops.addBinding.toInstance(classOf[MatMatchOp])
    ops.addBinding.toInstance(classOf[MatMatchingOp])
    ops.addBinding.toInstance(classOf[MMCReIdentOp])
    ops.addBinding.toInstance(classOf[CreateDataSetOp])

    ops.addBinding.toInstance(classOf[EnforceDayDurationOp])
    ops.addBinding.toInstance(classOf[MatMatchHeavyOp])
    ops.addBinding.toInstance(classOf[TemporalSplittingOp])
    ops.addBinding.toInstance(classOf[HeatMapGeneratorOp])
    ops.addBinding.toInstance(classOf[AreaCoverageMatrixOp])
   // ops.addBinding.toInstance(classOf[HeatMapConfusionOp])
    ops.addBinding.toInstance(classOf[HeatMapDistortionOp])
    ops.addBinding.toInstance(classOf[SelectPortionSplittingOp])
    ops.addBinding.toInstance(classOf[SelectRandomSamplingOp])
    ops.addBinding.toInstance(classOf[MultipleSplittingOp])
    ops.addBinding.toInstance(classOf[HybridLPPMPortionOp])
    ops.addBinding.toInstance(classOf[PoisExtractionOp])
    ops.addBinding.toInstance(classOf[HMConfusionOp])
    ops.addBinding.toInstance(classOf[HybridLPPMUtilityOp])
    ops.addBinding.toInstance(classOf[CountPassThroughOp])





  }
}