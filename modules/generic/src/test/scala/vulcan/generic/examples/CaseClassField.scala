/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import cats.Eq
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import vulcan.Codec
import vulcan.generic._

final case class CaseClassField(value: Int)

object CaseClassField {
  implicit val codec: Codec[CaseClassField] =
    Codec.derive

  implicit val caseClassFieldArbitrary: Arbitrary[CaseClassField] =
    Arbitrary(arbitrary[Int].map(CaseClassField(_)))

  implicit val caseClassFieldEq: Eq[CaseClassField] =
    Eq.fromUniversalEquals
}
