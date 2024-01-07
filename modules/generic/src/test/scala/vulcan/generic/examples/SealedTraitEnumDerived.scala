/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import cats.Eq
import org.scalacheck.{Arbitrary, Gen}
import vulcan.{AvroError, Codec}
import vulcan.generic._

@AvroNamespace("com.example")
sealed trait SealedTraitEnumDerived

object SealedTraitEnumDerived {
  implicit final val arbitrary: Arbitrary[SealedTraitEnumDerived] =
    Arbitrary(Gen.oneOf(FirstInSealedTraitEnumDerived, SecondInSealedTraitEnumDerived))

  implicit final val eq: Eq[SealedTraitEnumDerived] =
    Eq.fromUniversalEquals

  implicit final val codec: Codec[SealedTraitEnumDerived] =
    deriveEnum(
      symbols = List("first", "second"),
      encode = {
        case FirstInSealedTraitEnumDerived  => "first"
        case SecondInSealedTraitEnumDerived => "second"
      },
      decode = {
        case "first"  => Right(FirstInSealedTraitEnumDerived)
        case "second" => Right(SecondInSealedTraitEnumDerived)
        case other    => Left(AvroError(other))
      }
    )
}

case object FirstInSealedTraitEnumDerived extends SealedTraitEnumDerived

case object SecondInSealedTraitEnumDerived extends SealedTraitEnumDerived
