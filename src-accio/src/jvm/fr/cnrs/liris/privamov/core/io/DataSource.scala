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

package fr.cnrs.liris.privamov.core.io

/**
  * A source is responsible for reading elements. Each element is identified by a unique key.
  *
  * @tparam T Type of elements being read.
  */
trait DataSource[T] {
  /**
    * Return the list of the keys of elements available in this data source. Each key should be present only once,
    * but the list should be ordered in a deterministic order.
    */
  def keys: Seq[String]

  /**
    * Read the elements associated with a given key.
    *
    * @param key Key.
    */
  def read(key: String): Iterable[T]
}

/*
GO TO CODEC.scala
/**
 * A decoder converts a binary (record) into a plain object.
 *
 * @tparam T Plain object type
 */
trait Decoder[T] {
  /**
   * Decodes a binary record into an object.
   *
   * @param key   Key associated with the file containing this record
   * @param bytes Binary content
   * @return Plain object
   */
  def decode(key: String, bytes: Array[Byte]): Option[T]
}*/