package app

import cats.data.NonEmptyVector

import scala.collection.View

object ImplicitConversions:
  // We can't make this a value class because NonEmptyVector already is one.
  implicit class ToView[A](nev: NonEmptyVector[A]):
    def view: View[A] = nev.toVector.view
