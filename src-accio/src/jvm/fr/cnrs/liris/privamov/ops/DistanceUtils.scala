/*
 * Copyright LIRIS-CNRS (2017)
 * Contributors: Mohamed Maouche  <mohamed.maouchet@liris.cnrs.fr>
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

import scala.collection.Seq
import scala.util.Random

private[ops] object DistanceUtils {

  def d(m1: MatrixLight[Double], m2: MatrixLight[Double], typeD: Int): Double = {
    var d: Double = 0
    if (typeD > 0) {
      d = m1.minus(m2).norm(typeD)

    } else {
      typeD match {
        case -2 => d = dKL(m1, m2) // not symmetric
        case 0 => d = dNoneDet(m1, m2, 10, 10)
        case -3 => d = noneZeroCellsRatio(m1, m2)
        case -11 => d = dLoren(m1, m2)
        case -24 => d = dInnerP(m1, m2)
        case -26 => d = dcosine(m1, m2)
        case -31 => d = dDice(m1, m2)
        case -42 => d = dNdiv(m1, m2)
        case -46 => d = dclark(m1, m2)
        case -51 => d = dtopsoe(m1, m2)
        case -53 => d = dJensenDiv(m1, m2)
      }
    }
    d
  }

  def dConv2(m1: MatrixLight[Double], m2: MatrixLight[Double], d_i_1: Array[Double], d_j_1: Array[Double], avg1: Double, auto1: Double, d_i_2: Array[Double], d_j_2: Array[Double], avg2: Double, auto2: Double): Double = {
    var dist = 0.0
    val indices = m1.data.keys.toSet ++ m1.data.keys.toSet
    indices.foreach {
      case (i, j) =>
        dist = dist + (m1(i, j) - d_i_1(i) - d_j_1(j) + avg1) * (m2(i, j) - d_i_2(i) - d_j_2(j) + avg2)
    }
    dist = dist / (m1.nbRow * m1.nbCol)
    dist = dist / math.sqrt(auto1 * auto2)
    dist
  }

  def noneZeroCellsRatio[U](unknownMat: MatrixLight[U], knownMat: MatrixLight[U]): Double = {
    val nb1 = unknownMat.data.keys.size + knownMat.data.keys.size
    val nb2 = unknownMat.data.keys.toSet.intersect(knownMat.data.keys.toSet).size
    1 - nb2.toDouble / nb1.toDouble
  }

  def dNoneDet(unknownMat: MatrixLight[Double], knownMat: MatrixLight[Double], k: Int, x: Int): Double = {
    var d: Double = 0.0
    // random number generator
    val rnd = new Random()
    // all the non zeros indices
    val noneEmptyCellsArray: Array[(Int, Int)] = unknownMat.data.keys.toArray

    val k2 = Math.min(k, noneEmptyCellsArray.length)
    var tmpx = 0
    // repeat x times the process
    while (tmpx < x) {
      // this iteration chosen cells
      var chosenCells: Seq[(Int, Int)] = Seq[(Int, Int)]()
      var tmpK = 0
      val mask = Array.fill[Boolean](noneEmptyCellsArray.length)(false)
      while (tmpK < k2) {
        val ind: Int = rnd.nextInt(noneEmptyCellsArray.length)
        // println(s" tmpk/k =  $tmpK/$k2 ;  ind = $ind ; tmpx/x = $tmpx/$x ")
        if (!mask(ind)) {
          mask(ind) = true
          chosenCells = chosenCells :+ noneEmptyCellsArray(ind)
          tmpK += 1
        }
      }
      d = d + vectorDistance(chosenCells, unknownMat, knownMat)
      tmpx += 1
      // println(s"chosenCells = $chosenCells")
    }
    d = d / x
    d
  }

  def vectorDistance(chosenCells: Seq[(Int, Int)], unknownMat: MatrixLight[Double], knownMat: MatrixLight[Double]): Double = {
    var d: Double = 0.0
    chosenCells.foreach {
      ind =>
        d += Math.pow(unknownMat(ind._1, ind._2) - knownMat(ind._1, ind._2), 2)
    }
    d = math.sqrt(d)
    d
  }


  def dJensenDiv(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var top = 0.0
    var ln1 = 0.0
    var ln2 = 0.0
    var ln12 = 0.0

    var d1 = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>
        ln1 = if (m1(i, j) == 0) 0 else m1(i, j) * math.log(m1(i, j))
        ln2 = if (m2(i, j) == 0) 0 else m2(i, j) * math.log(m2(i, j))
        ln12 = if (m1(i, j) == 0 && m2(i, j) == 0) 0 else ((m1(i, j) + m2(i, j)) / 2) * math.log((m1(i, j) + m2(i, j)) / 2)
        d1 = d1 + (ln1 + ln2) / 2 - ln12
    }
    d1
  }

  def dtopsoe(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var top = 0.0
    var ln1 = 0.0
    var ln2 = 0.0
    var d1 = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>
        ln1 = if (m1(i, j) == 0) 0 else m1(i, j) * math.log(2 * m1(i, j) / (m1(i, j) + m2(i, j)))
        ln2 = if (m2(i, j) == 0) 0 else m2(i, j) * math.log(2 * m2(i, j) / (m1(i, j) + m2(i, j)))
        d1 = d1 + ln1 + ln2
    }
    d1
  }

  def dDice(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var top = 0.0
    var low1 = 0.0
    var low2 = 0.0
    var d1 = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>
        top = top + (m1(i, j) - m2(i, j)) * (m1(i, j) - m2(i, j))
        low1 = low1 + m1(i, j) * m1(i, j)
        low2 = low2 + m2(i, j) * m2(i, j)
    }
    d1 = top / (low1 + low2)
    d1
  }

  def dInnerP(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var d1 = 0.0
    val indices = m1.data.keys.toSet.intersect(m2.data.keys.toSet)
    indices.foreach {
      case (i: Int, j: Int) =>
        d1 = d1 + m1(i, j) * m2(i, j)
    }
    1 - d1
  }

  def dNdiv(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var tmp = 0.0
    var d1 = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>

        tmp = (m1(i, j) + m2(i, j)) * (m1(i, j) + m2(i, j))
        if (m1(i, j) != 0.0) tmp = tmp / m1(i, j)
        else tmp = tmp / 1E-10
        d1 = d1 + tmp
    }
    d1
  }

  def dLoren(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var tmp = 0.0
    var d1 = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>
        tmp = math.abs(m1(i, j) - m2(i, j))
        d1 = d1 + math.log(1 + tmp)
    }
    d1
  }

  def dclark(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var top = 0.0
    var low = 0.0
    var d1 = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>
        top = math.abs(m1(i, j) - m2(i, j))
        low = m1(i, j) + m2(i, j)
        d1 = d1 + (top / low) * (top / low)

    }
    math.sqrt(d1)
  }


  def dcosine(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var d12 = 0.0
    var d11 = 0.0
    var d22 = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>
        d12 = d12 + m1(i, j) * m2(i, j)
        d11 = d11 + m1(i, j) * m1(i, j)
        d22 = d22 + m2(i, j) * m2(i, j)
    }
    1 - d12 / math.sqrt(d11 * d22)
  }

  def dcov2(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    val nbRow = m1.nbRow
    val nbCol = m1.nbCol
    var dc = 0.0
    val d_i_1 = Array.fill[Double](nbRow)(0)
    val d_i_2 = Array.fill[Double](nbRow)(0)
    val d_j_1 = Array.fill[Double](nbCol)(0)
    val d_j_2 = Array.fill[Double](nbCol)(0)
    var d1 = 0.0
    var d2 = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    var set_i = Set[Int]()
    var set_j = Set[Int]()
    indices.foreach {
      case (i: Int, j: Int) =>
        set_i += i
        set_j += j
        d_i_1(i) = d_i_1(i) + m1(i, j)
        d_i_2(i) = d_i_2(i) + m2(i, j)
        d_j_1(j) = d_j_1(j) + m1(i, j)
        d_j_2(j) = d_j_2(j) + m2(i, j)
        d1 += m1(i, j)
        d2 += m2(i, j)
    }
    for (i <- set_i) {
      d_i_1(i) = d_i_1(i) / m1.nbCol.toDouble
      d_i_2(i) = d_i_2(i) / m2.nbCol.toDouble
    }
    for (j <- set_j) {
      d_j_1(j) = d_j_1(j) / m1.nbRow.toDouble
      d_j_2(j) = d_j_2(j) / m2.nbRow.toDouble
    }
    d1 = d1 / (nbRow * nbCol).toDouble
    d2 = d2 / (nbRow * nbCol).toDouble
    for (i <- set_i) {
      for (j <- set_j) {
        dc = dc + (m1(i, j) - d_i_1(i) - d_j_1(j) - d1) * (m2(i, j) - d_i_2(i) - d_j_2(j) - d2)
      }
    }
    dc = dc / (nbRow * nbCol)
    dc
  }


  def dKL(m1: MatrixLight[Double], m2: MatrixLight[Double]): Double = {
    var dc = 0.0
    var ln1 = 0.0
    var tmp = 0.0
    val indices = m1.data.keys.toSet ++ m2.data.keys.toSet
    indices.foreach {
      case (i: Int, j: Int) =>
        if (m1(i, j) == 0) tmp = 0
        else {
          tmp = if (m2(i, j) == 0) m1(i, j) * math.log(m1(i, j) / 1E-10) else m1(i, j) * math.log(m1(i, j) / m2(i, j))
        }
        dc = dc + tmp
    }
    dc
  }

  def dotProduct(m1: MatrixLight[Double], m2: MatrixLight[Double]): MatrixLight[Double] = {
    val newMat = new MatrixLight[Double](m1.nbRow, m1.nbCol)
    val indices = m1.data.keys.toSet.intersect(m2.data.keys.toSet)
    indices.foreach {
      case (i: Int, j: Int) => newMat.assign(i,j, m1(i,j)*m2(i,j))
    }
    newMat
  }

  def complement(m : MatrixLight[Double]) : MatrixLight[Double] = {
    m.transform{( pair : (Int,Int),v : Double) => 1 - v}
  }

}