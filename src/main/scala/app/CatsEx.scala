package app

import cats.{Applicative, Functor, Monad, Monoid}
import cats.implicits.*

import scala.annotation.tailrec

object CatsEx:
  final case class Neo[T](n: T)

  given [T: Monoid]: Monoid[Neo[T]] with
    override def combine(x: Neo[T], y: Neo[T]): Neo[T] = Neo(x.n |+| y.n)
    override def empty: Neo[T] = Neo(summon[Monoid[T]].empty)

  val neo1: Neo[Int] = Neo(1)
  val neo2: Neo[Int] = Neo(2)
  val neo3: Neo[Int] = neo1 |+| neo2

  given neoFunctor: Functor[Neo] with
    override def map[A, B](na: Neo[A])(f: A => B): Neo[B] =
      Neo[B](f(na.n))

  given neoApp: Applicative[Neo] with
    override def pure[A](a: A): Neo[A] = Neo(a)
    override def ap[A, B](f: Neo[A => B])(fa: Neo[A]): Neo[B] =
      Neo(f.n(fa.n))

  given neoMonad: Monad[Neo] with
    override def flatMap[A, B](na: Neo[A])(f: A => Neo[B]): Neo[B] =
      f(na.n)

    override def pure[A](a: A): Neo[A] = Neo(a)

    @tailrec
    final override def tailRecM[A, B](a: A)(f: A => Neo[Either[A, B]]): Neo[B] =
      f(a) match {
        case Neo(Left(x)) => tailRecM(x)(f)
        case Neo(Right(y)) => Neo(y)
      }

  given neoOptFunctor: Functor[Option] with
    override def map[A, B](opt: Option[A])(f: A => B): Option[B] =
      opt.map(f)

  given neoOptApplicative: Applicative[Option] with
    override def pure[A](a: A): Option[A] = Some(a)
    override def ap[A, B](fOpt: Option[A => B])(optA: Option[A]): Option[B] =
      (fOpt, optA) match {
        case (Some(f), Some(a)) => Some(f(a))
        case _ => None
      }

  given neoOptMonad: Monad[Option] with
    override def pure[A](a: A): Option[A] = Some(a)

    override def flatMap[A, B](fa: Option[A])(f: A => Option[B]): Option[B] =
      fa.fold(None)(f)

    @tailrec
    final override def tailRecM[A, B](a: A)(f: A => Option[Either[A, B]]): Option[B] =
      f(a) match {
        case Some(Left(x)) => tailRecM(x)(f)
        case Some(Right(x)) => Some(x)
        case None => None
      }
