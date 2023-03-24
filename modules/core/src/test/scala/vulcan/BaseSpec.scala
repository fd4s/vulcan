/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalactic.Equality
import cats.kernel.Eq
import scala.reflect.ClassTag

class BaseSpec extends AnyFunSpec with ScalaCheckPropertyChecks with EitherValues {

  // In Scala 3, `===` uses Scalactic rather than Cats syntax
  implicit def catsEquality[A: Eq: ClassTag]: Equality[A] = new Equality[A] {
    def areEqual(a: A, b: Any): Boolean = b match {
      case b: A => Eq[A].eqv(a, b)
      case _    => false
    }
  }
}
