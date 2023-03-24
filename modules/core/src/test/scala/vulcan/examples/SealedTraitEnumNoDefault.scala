/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.examples

import cats.Eq
import org.scalacheck.{Arbitrary, Gen}
import vulcan.{AvroError, Codec, Props}

sealed trait SealedTraitEnumNoDefault

object SealedTraitEnumNoDefault {
  implicit final val arbitrary: Arbitrary[SealedTraitEnumNoDefault] =
    Arbitrary(Gen.oneOf(FirstInSealedTraitEnumNoDefault, SecondInSealedTraitEnumNoDefault))

  implicit final val eq: Eq[SealedTraitEnumNoDefault] =
    Eq.fromUniversalEquals

  implicit final val codec: Codec[SealedTraitEnumNoDefault] =
    Codec.enumeration(
      name = "SealedTraitEnumNoDefault",
      namespace = "vulcan.examples",
      symbols = List("first", "second"),
      aliases = List("first", "second"),
      doc = Some("documentation"),
      default = None,
      encode = {
        case FirstInSealedTraitEnumNoDefault  => "first"
        case SecondInSealedTraitEnumNoDefault => "second"
      },
      decode = {
        case "first"  => Right(FirstInSealedTraitEnumNoDefault)
        case "second" => Right(SecondInSealedTraitEnumNoDefault)
        case other    => Left(AvroError(other))
      },
      props = Props.one("custom1", 10).add("custom2", "value2")
    )
}

case object FirstInSealedTraitEnumNoDefault extends SealedTraitEnumNoDefault

case object SecondInSealedTraitEnumNoDefault extends SealedTraitEnumNoDefault
