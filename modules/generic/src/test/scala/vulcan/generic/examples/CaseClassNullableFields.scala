package vulcan.generic.examples

import cats.Eq
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Arbitrary.arbitrary
import vulcan.Codec
import vulcan.generic._

import java.time.LocalDate

final case class CaseClassNullableFields(
  int: Option[Int],
  long: Option[Long],
  string: Option[String],
  date: Option[LocalDate],
  map: Option[Map[String, String]],
  caseClassValueClass: Option[CaseClassValueClass],
  sealedTraitEnumDerived: Option[SealedTraitEnumDerived]
)

object CaseClassNullableFields {
  implicit val caseClassNullableFieldsArbitrary: Arbitrary[CaseClassNullableFields] = {
    implicit val arbitraryLocalDate: Arbitrary[LocalDate] =
      Arbitrary(Gen.posNum[Int].map(d => LocalDate.ofEpochDay(d.toLong)))

    val gen = for {
      int <- arbitrary[Option[Int]]
      long <- arbitrary[Option[Long]]
      string <- arbitrary[Option[String]]
      date <- arbitrary[Option[LocalDate]]
      map <- arbitrary[Option[Map[String, String]]]
      caseClassValueClass <- arbitrary[Option[Int]].map(_.map(CaseClassValueClass.apply))
      sealedTraitEnumDerived <- arbitrary[Option[SealedTraitEnumDerived]]
    } yield apply(int, long, string, date, map, caseClassValueClass, sealedTraitEnumDerived)
    Arbitrary(gen)
  }

  implicit val caseClassAvroNamespaceEq: Eq[CaseClassNullableFields] =
    Eq.fromUniversalEquals

  implicit val codec: Codec[CaseClassNullableFields] =
    Codec.derive
}
