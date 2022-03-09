package vulcan.generic.examples

import cats.Eq
import org.scalacheck.Arbitrary
import vulcan.Codec
import vulcan.generic._

import java.time.LocalDate

@AvroNullDefault(true)
final case class CaseClassAvroNullDefault(
  int: Option[Int],
  long: Option[Long],
  string: Option[String],
  date: Option[LocalDate],
  map: Option[Map[String, String]],
  caseClassValueClass: Option[CaseClassValueClass],
  sealedTraitEnumDerived: Option[SealedTraitEnumDerived]
)

object CaseClassAvroNullDefault {

  implicit val caseClassAvroNullDefaultArbitrary: Arbitrary[CaseClassAvroNullDefault] =
    Arbitrary(CaseClassNullableFields.genFieldProduct.map((apply _).tupled))

  implicit val caseClassAvroNamespaceEq: Eq[CaseClassAvroNullDefault] =
    Eq.fromUniversalEquals

  implicit val codec: Codec[CaseClassAvroNullDefault] =
    Codec.derive
}
