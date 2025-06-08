package app

import cats.{Applicative, Functor}
import cats.data.NonEmptyVector
import cats.syntax.functor.*

import scala.collection.View

object ImplicitConversions:
  // We can't make this a value class because NonEmptyVector already is one.
  extension [A](nev: NonEmptyVector[A]) {
    inline def view: View[A] = nev.toVector.view
  }

  extension [F[_], G[_]: Functor, O](s: fs2.Stream.CompileOps[F, G, O])
    inline def theLast(using fs2.Compiler[F, G]): G[O] =
      s.last.map(_.get)

  extension [F[_]: Applicative as app, A](b: Boolean)
    inline def whenA(fa: F[A]): F[Unit] =
      import cats.syntax.all.*
      fa.whenA(b)(using app)

  extension (obj: Any)
    inline def safeAs[C]: Option[C] = obj match {
      case c: C => Some(c)
      case _ => None
    }
