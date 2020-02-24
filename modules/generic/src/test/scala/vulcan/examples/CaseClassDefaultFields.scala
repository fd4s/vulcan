package vulcan.examples

import cats.Eq
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import vulcan.Codec;
import vulcan.generic._

final case class CaseClassDefaultFields(
  name: String = "Pikachu",
  age: Option[Int] = None
)

object CaseClassDefaultFields {
  implicit val codec: Codec[CaseClassDefaultFields] = Codec.derive

  implicit val caseClassFieldArbitrary: Arbitrary[CaseClassDefaultFields] =
    Arbitrary {
      for {
        arbString <- arbitrary[String]
        arbInt <- arbitrary[Option[Int]]
      } yield CaseClassDefaultFields(arbString, arbInt)
    }

  implicit val caseClassFieldEq: Eq[CaseClassDefaultFields] = Eq.fromUniversalEquals
}
