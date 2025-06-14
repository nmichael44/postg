package app

import cats.{Applicative, Functor}
import cats.data.{EitherT, NonEmptyVector}
import cats.syntax.functor.*

import scala.collection.View

object ImplicitConversions:
  extension [A](nev: NonEmptyVector[A]) {
    inline def view: View[A] = nev.toVector.view
  }

  extension [F[_], G[_]: Functor, O](s: fs2.Stream.CompileOps[F, G, O]) {
    inline def theLast(using fs2.Compiler[F, G]): G[O] =
      s.last.map(_.get)
  }

  extension [F[_]: Applicative as app, A](b: Boolean) {
    inline def whenA(fa: F[A]): F[Unit] =
      import cats.syntax.all.*
      fa.whenA(b)(using app)
  }

  extension (obj: Any) {
    inline def safeAs[C]: Option[C] = obj match {
      case c: C => Some(c)
      case _ => None
    }
  }

  extension [F[_]: Functor, A](fa: F[A]) {
    inline def lift[B]: EitherT[F, B, A] = EitherT.liftF[F, B, A](fa)
  }

  extension [F[_], A, B](fe: F[Either[A, B]]) {
    inline def toEitherT: EitherT[F, A, B] = EitherT(fe)
  }

  extension [F[_]: Applicative, A, B](e: Either[A, B]) {
    inline def toEitherT: EitherT[F, A, B] = EitherT.fromEither(e)
  }

  extension [F[_]: Functor, A](o: F[Option[A]]) {
    inline def toEitherT[B](ifNone: => B): EitherT[F, B, A] = EitherT.fromOptionF(o, ifNone)
  }
