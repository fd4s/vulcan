/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import cats.kernel.laws.discipline.EqTests
import cats.tests.CatsSuite
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary

final class AvroErrorEqSpec extends CatsSuite {
  implicit val avroErrorArbitrary: Arbitrary[AvroError] =
    Arbitrary(arbitrary[String].map(AvroError(_)))

  implicit val avroErrorAvroErrorArbitrary: Arbitrary[AvroError => AvroError] =
    Arbitrary {
      arbitrary[String => String].map { f => (error: AvroError) =>
        AvroError(f(error.message))
      }
    }

  checkAll("AvroError", EqTests[AvroError].eqv)
}
