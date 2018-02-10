package fr.cnrs.liris.privamov.gateway.auth

import fr.cnrs.liris.privamov.core.model.Event
import fr.cnrs.liris.common.geo.BoundingBox
import org.joda.time.DateTime

case class View(source: Option[String] = None, startAfter: Option[DateTime] = None, endBefore: Option[DateTime] = None, area: Option[BoundingBox] = None) {
  def accessible(id: String): Boolean = source.contains(id) || source.isEmpty

  def filter(events: Seq[Event]): Seq[Event] = {
    if (source.isDefined || startAfter.isDefined || endBefore.isDefined || area.isDefined) {
      events.filter(filter)
    } else {
      events
    }
  }

  def restrict(restrictions: Iterable[View]): Set[View] = {
    if (restrictions.exists(_.includes(this))) {
      Set(this)
    } else {
      restrictions.flatMap(_.restrict(this)).toSet
    }
  }

  def restrict(other: View): Option[View] = {
    val newSource = reconciliate(source, other.source) { (a, b) =>
      if (a == b) Some(Some(a)) else None
    } match {
      case Some(s) => s
      case None => return None
    }
    val newStartAfter = reconciliate(startAfter, other.startAfter) { (a, b) =>
      Some(Some(max(a, b)))
    } match {
      case Some(d) => d
      case None => return None
    }
    val newEndBefore = reconciliate(endBefore, other.endBefore) { (a, b) =>
      Some(Some(min(a, b)))
    } match {
      case Some(d) => d
      case None => return None
    }
    val newArea = reconciliate(area, other.area) { (a, b) =>
      if (a.intersects(b)) Some(Some(a.intersection(b))) else None
    } match {
      case Some(a) => a
      case None => return None
    }
    Some(View(newSource, newStartAfter, newEndBefore, newArea))
  }

  def includes(other: View): Boolean = {
    (source.isEmpty || other.source.contains(source.get)) &&
        (startAfter.isEmpty || other.startAfter.forall(o => o == startAfter.get || o.isAfter(startAfter.get))) &&
        (endBefore.isEmpty || other.endBefore.forall(o => o == endBefore.get || o.isBefore(endBefore.get))) &&
        (area.isEmpty || other.area.forall(_.contained(area.get)))
  }

  private def max(a: DateTime, b: DateTime) = if (a.isAfter(b)) a else b

  private def min(a: DateTime, b: DateTime) = if (a.isBefore(b)) a else b

  private def reconciliate[T](a: Option[T], b: Option[T])(fn: (T, T) => Option[Option[T]]): Option[Option[T]] = {
    if (a.isEmpty) {
      Some(b)
    } else if (b.isEmpty) {
      Some(a)
    } else {
      fn(a.get, b.get)
    }
  }

  private def filter(event: Event) = {
    startAfter.forall(event.time.isAfter) && endBefore.forall(event.time.isBefore) &&
        area.forall(_.contains(event.point)) && source.forall(_.contains(event.user))
  }
}

object View {
  def everything: View = new View()
}