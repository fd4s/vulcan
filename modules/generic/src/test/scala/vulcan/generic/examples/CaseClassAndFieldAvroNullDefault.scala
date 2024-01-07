/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic.examples

import cats.Eq
import org.scalacheck.Arbitrary
import vulcan.Codec
import vulcan.generic._

import java.time.LocalDate

@AvroNullDefault(true)
final case class CaseClassAndFieldAvroNullDefault(
  @AvroNullDefault(false) int: Option[Int],
  long: Option[Long],
  @AvroNullDefault(false) string: Option[String],
  date: Option[LocalDate],
  @AvroNullDefault(false) map: Option[Map[String, String]],
  caseClassValueClass: Option[CaseClassValueClass],
  @AvroNullDefault(false) sealedTraitEnumDerived: Option[SealedTraitEnumDerived]
)

object CaseClassAndFieldAvroNullDefault {
  implicit val caseClassAvroNullDefaultArbitrary: Arbitrary[CaseClassAndFieldAvroNullDefault] =
    Arbitrary(CaseClassNullableFields.genFieldProduct.map((apply _).tupled))

  implicit val caseClassAvroNamespaceEq: Eq[CaseClassAndFieldAvroNullDefault] =
    Eq.fromUniversalEquals

  implicit val codec: Codec[CaseClassAndFieldAvroNullDefault] =
    Codec.derive
}
