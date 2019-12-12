/*
 * Copyright 2019 OVO Energy Limited
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
sealed abstract class Prism[S, A] {

  /** Attempts to select a coproduct part. */
  def getOption: S => Option[A]

  /** Creates a coproduct from a coproduct part. */
  def reverseGet: A => S
}

final object Prism {

  /**
    * Returns the [[Prism]] for the specified types.
    */
  final def apply[S, A](implicit prism: Prism[S, A]): Prism[S, A] =
    prism

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
    * Returns a new [[Prism]] instance using the specified
    * `get` partial function and `reverseGet` function.
    */
  final def partial[S, A](get: PartialFunction[S, A])(reverseGet: A => S): Prism[S, A] =
    Prism.instance(get.lift)(reverseGet)
}
