/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.examples

import cats.Eq
import org.scalacheck.{Arbitrary, Gen}
import vulcan.{AvroError, Codec, Props}

sealed trait SealedTraitEnum

object SealedTraitEnum {
  implicit final val arbitrary: Arbitrary[SealedTraitEnum] =
    Arbitrary(Gen.oneOf(FirstInSealedTraitEnum, SecondInSealedTraitEnum))

  implicit final val eq: Eq[SealedTraitEnum] =
    Eq.fromUniversalEquals

  implicit final val codec: Codec[SealedTraitEnum] =
    Codec.enumeration(
      name = "SealedTraitEnum",
      namespace = "vulcan.examples",
      symbols = List("first", "second"),
      aliases = List("first", "second"),
      doc = Some("documentation"),
      default = Some(FirstInSealedTraitEnum),
      encode = {
        case FirstInSealedTraitEnum  => "first"
        case SecondInSealedTraitEnum => "second"
      },
      decode = {
        case "first"  => Right(FirstInSealedTraitEnum)
        case "second" => Right(SecondInSealedTraitEnum)
        case other    => Left(AvroError(other))
      },
      props = Props.one("custom1", 10).add("custom2", "value2")
    )
}

case object FirstInSealedTraitEnum extends SealedTraitEnum

case object SecondInSealedTraitEnum extends SealedTraitEnum
