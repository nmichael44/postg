package app

import cats.data.NonEmptyVector
import cats.syntax.functor.*
import cats.Functor

import scala.collection.View

object ImplicitConversions:
  // We can't make this a value class because NonEmptyVector already is one.
  extension [A](nev: NonEmptyVector[A]) {
    def view: View[A] = nev.toVector.view
  }

  extension [F[_], G[_]: Functor, O](s: fs2.Stream.CompileOps[F, G, O])
    def theLast(using fs2.Compiler[F, G]): G[O] =
      s.last.map(_.get)
