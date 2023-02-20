/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

/**
  * Optic used for selecting a part of a coproduct type.
  */
@implicitNotFound(
  "could not find implicit Prism[${S}, ${A}]; ensure ${A} is a subtype of ${S} or manually define an instance"
)
sealed abstract class Prism[S, A] { self =>

  /** Attempts to select a coproduct part. */
  def getOption: S => Option[A]

  /** Creates a coproduct from a coproduct part. */
  def reverseGet: A => S

  def imap[S0](f: S0 => Option[S], g: S => S0): Prism[S0, A] = new Prism[S0, A] {
    override def getOption: S0 => Option[A] = f(_).flatMap(self.getOption)
    override def reverseGet: A => S0 = a => g(self.reverseGet(a))
  }
}

object Prism extends PrismLowPriority {

  /**
    * Returns the [[Prism]] for the specified types.
    */
  final def apply[S, A](implicit prism: Prism[S, A]): Prism[S, A] =
    prism

  /**
    * Returns a new [[Prism]] for the specified type.
    */
  implicit final def identity[A]: Prism[A, A] =
    Prism.instance[A, A](Some(_))(a => a)

  /**
    * Returns a new [[Prism]] instance using the specified
    * `getOption` and `reverseGet` functions.
    */
  final def instance[S, A](getOption: S => Option[A])(reverseGet: A => S): Prism[S, A] = {
    val _getOption = getOption
    val _reverseGet = reverseGet

    new Prism[S, A] {
      override final val getOption: S => Option[A] =
        _getOption

      override final val reverseGet: A => S =
        _reverseGet

      override final def toString: String =
        "Prism$" + System.identityHashCode(this)
    }
  }

  /**
    * Returns a new [[Prism]] from `Either[A, B]` to `Left[A, B]`.
    */
  implicit final def left[A, B]: Prism[Either[A, B], Left[A, B]] =
    Prism.instance[Either[A, B], Left[A, B]] {
      case left @ Left(_) => Some(left)
      case Right(_)       => None
    }(left => left)

  /**
    * Returns a new [[Prism]] from `Option` to `None`.
    */
  implicit final def none[A]: Prism[Option[A], None.type] =
    Prism.instance[Option[A], None.type] {
      case None    => Some(None)
      case Some(_) => None
    }(none => none)

  /**
    * Returns a new [[Prism]] instance using the specified
    * `get` partial function and `reverseGet` function.
    */
  final def partial[S, A](get: PartialFunction[S, A])(reverseGet: A => S): Prism[S, A] =
    Prism.instance(get.lift)(reverseGet)

  /**
    * Returns a new [[Prism]] from `Either[A, B]` to `Right[A, B]`.
    */
  implicit final def right[A, B]: Prism[Either[A, B], Right[A, B]] =
    Prism.instance[Either[A, B], Right[A, B]] {
      case Left(_)          => None
      case right @ Right(_) => Some(right)
    }(right => right)

  /**
    * Returns a new [[Prism]] from `Option` to `Some`.
    */
  implicit final def some[S, A](implicit prism: Prism[S, A]): Prism[Option[S], Some[A]] =
    Prism.instance[Option[S], Some[A]] {
      case None    => None
      case Some(s) => prism.getOption(s).map(Some(_))
    }(_.map(prism.reverseGet))
}

private[vulcan] sealed abstract class PrismLowPriority {

  /**
    * Returns a new [[Prism]] for the specified supertype
    * and subtype.
    *
    * Relies on class tags. Since the function is implicit,
    * [[Prism]]s are available implicitly for any supertype
    * and subtype relationships.
    */
  implicit final def derive[S, A <: S](
    implicit tag: ClassTag[A]
  ): Prism[S, A] = {
    val getOption = (s: S) =>
      if (tag.runtimeClass.isInstance(s))
        Some(s.asInstanceOf[A])
      else None

    val reverseGet = (a: A) => (a: S)

    Prism.instance(getOption)(reverseGet)
  }
}
