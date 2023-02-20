/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import cats.Eq
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import vulcan.Codec
import vulcan.generic._

sealed trait SealedTraitCaseClassCustom

final case class FirstInSealedTraitCaseClassCustom(value: Int) extends SealedTraitCaseClassCustom

final case class SecondInSealedTraitCaseClassCustom(value: String)
    extends SealedTraitCaseClassCustom

object SecondInSealedTraitCaseClassCustom {
  implicit val codec: Codec[SecondInSealedTraitCaseClassCustom] =
    Codec[String].imap(apply)(_.value)
}

object SealedTraitCaseClassCustom {
  implicit val sealedTraitCaseClassCustomArbitrary: Arbitrary[SealedTraitCaseClassCustom] =
    Arbitrary(
      Gen.oneOf(
        arbitrary[Int].map(FirstInSealedTraitCaseClassCustom(_)),
        arbitrary[String].map(SecondInSealedTraitCaseClassCustom(_))
      )
    )

  implicit val sealedTraitCaseClassCustomEq: Eq[SealedTraitCaseClassCustom] =
    Eq.fromUniversalEquals

  implicit val codec: Codec[SealedTraitCaseClassCustom] =
    Codec.derive
}
