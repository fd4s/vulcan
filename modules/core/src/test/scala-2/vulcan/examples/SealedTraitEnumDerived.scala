/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.examples

import scala.annotation.nowarn
import cats.Eq
import org.scalacheck.{Arbitrary, Gen}
import vulcan.{AvroError, AvroNamespace, Codec}

@AvroNamespace("com.example")
@nowarn("msg=deprecated")
sealed trait SealedTraitEnumDerived

object SealedTraitEnumDerived {
  implicit final val arbitrary: Arbitrary[SealedTraitEnumDerived] =
    Arbitrary(Gen.oneOf(FirstInSealedTraitEnumDerived, SecondInSealedTraitEnumDerived))

  implicit final val eq: Eq[SealedTraitEnumDerived] =
    Eq.fromUniversalEquals

  @nowarn("msg=deprecated")
  implicit final val codec: Codec[SealedTraitEnumDerived] =
    Codec.deriveEnum(
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
