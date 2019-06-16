package vulcan.examples

import cats.Eq
import vulcan.Codec
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary

final case class CaseClassField(value: Int)

object CaseClassField {
  implicit val codec: Codec[CaseClassField] =
    Codec.derive

  implicit val caseClassFieldArbitrary: Arbitrary[CaseClassField] =
    Arbitrary(arbitrary[Int].map(CaseClassField(_)))

  implicit val caseClassFieldEq: Eq[CaseClassField] =
    Eq.fromUniversalEquals
}
