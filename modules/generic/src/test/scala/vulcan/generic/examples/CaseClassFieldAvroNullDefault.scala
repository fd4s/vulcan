/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import cats.Eq
import org.scalacheck.Arbitrary
import vulcan.Codec
import vulcan.generic._

import java.time.LocalDate

final case class CaseClassFieldAvroNullDefault(
  @AvroNullDefault(true) int: Option[Int],
  long: Option[Long],
  @AvroNullDefault(true) string: Option[String],
  date: Option[LocalDate],
  @AvroNullDefault(true) map: Option[Map[String, String]],
  caseClassValueClass: Option[CaseClassValueClass],
  @AvroNullDefault(true) sealedTraitEnumDerived: Option[SealedTraitEnumDerived]
)

object CaseClassFieldAvroNullDefault {
  implicit val caseClassAvroNullDefaultArbitrary: Arbitrary[CaseClassFieldAvroNullDefault] =
    Arbitrary(CaseClassNullableFields.genFieldProduct.map((apply _).tupled))

  implicit val caseClassAvroNamespaceEq: Eq[CaseClassFieldAvroNullDefault] =
    Eq.fromUniversalEquals

  implicit val codec: Codec[CaseClassFieldAvroNullDefault] =
    Codec.derive
}
