package vulcan.generic.examples

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}

import java.time.LocalDate

object CaseClassNullableFields {
  type FieldProduct = (
    Option[Int],
    Option[Long],
    Option[String],
    Option[LocalDate],
    Option[Map[String, String]],
    Option[CaseClassValueClass],
    Option[SealedTraitEnumDerived]
  )
  val genFieldProduct: Gen[FieldProduct] = {
    implicit val arbitraryLocalDate: Arbitrary[LocalDate] =
      Arbitrary(Gen.posNum[Int].map(d => LocalDate.ofEpochDay(d.toLong)))

    for {
      int <- arbitrary[Option[Int]]
      long <- arbitrary[Option[Long]]
      string <- arbitrary[Option[String]]
      date <- arbitrary[Option[LocalDate]]
      map <- arbitrary[Option[Map[String, String]]]
      caseClassValueClass <- arbitrary[Option[Int]].map(_.map(CaseClassValueClass.apply))
      sealedTraitEnumDerived <- arbitrary[Option[SealedTraitEnumDerived]]
    } yield (int, long, string, date, map, caseClassValueClass, sealedTraitEnumDerived)
  }
}
