package vulcan.generic.examples

import cats.Eq
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import vulcan.Codec
import vulcan.generic._

final case class CaseClassTypeParameterField[T](s: String, value: T)
final case class CaseClassInner[T, S](inner1: T, inner2: S)

object CaseClassTypeParameterField {
  implicit val configuration: Configuration = avro4s.avro4sGenericConfiguration

  implicit val intCodec: Codec[CaseClassTypeParameterField[Int]] =
    Codec.derive

  implicit val longCodec: Codec[CaseClassTypeParameterField[Long]] =
    Codec.derive

  implicit val innerIntCodec: Codec[CaseClassInner[Int, Long]] =
    Codec.derive

  implicit val withInnerIntCodec: Codec[CaseClassTypeParameterField[CaseClassInner[Int, Long]]] =
    Codec.derive

  implicit val caseClassTypeParameterFieldArbitrary: Arbitrary[CaseClassTypeParameterField[Int]] =
    Arbitrary(for {
      s <- arbitrary[String]
      i <- arbitrary[Int]
    } yield CaseClassTypeParameterField(s, i))

  implicit val caseClassTypeParameterFieldEq: Eq[CaseClassTypeParameterField[Int]] =
    Eq.fromUniversalEquals
}
