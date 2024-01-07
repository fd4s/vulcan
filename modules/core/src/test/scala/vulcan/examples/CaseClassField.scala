/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.examples

import cats.Eq
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import vulcan.Codec

final case class CaseClassField(value: Int)

object CaseClassField {
  implicit val codec: Codec[CaseClassField] =
    Codec.record("CaseClassField", "") { field =>
      field("value", _.value).map(CaseClassField(_))
    }

  implicit val caseClassFieldArbitrary: Arbitrary[CaseClassField] =
    Arbitrary(arbitrary[Int].map(CaseClassField(_)))

  implicit val caseClassFieldEq: Eq[CaseClassField] =
    Eq.fromUniversalEquals
}
