/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import cats.Eq
import cats.implicits._
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Arbitrary.arbitrary
import shapeless.{:+:, CNil, Coproduct}
import vulcan._
import vulcan.generic.examples._

final class CoproductRoundtripSpec extends RoundtripBase {
  describe("coproduct") {
    type Types = CaseClassField :+: Int :+: CaseClassAvroDoc :+: CNil

    implicit val arbitraryTypes: Arbitrary[Types] =
      Arbitrary {
        Gen.oneOf(
          arbitrary[Int].map(n => Coproduct[Types](CaseClassField(n))),
          arbitrary[Int].map(n => Coproduct[Types](n)),
          arbitrary[Option[String]].map(os => Coproduct[Types](CaseClassAvroDoc(os)))
        )
      }

    implicit val eqTypes: Eq[Types] =
      Eq.fromUniversalEquals

    it("roundtrip.derived") {
      roundtrip[Types]
    }

    it("roundtrip.union") {
      implicit val codec: Codec[Types] =
        Codec.union { alt =>
          alt[CaseClassField] |+| alt[Int] |+| alt[CaseClassAvroDoc]
        }

      roundtrip[Types]
    }
  }
}
