package vulcan.generic.examples

import cats.Eq
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Arbitrary.arbitrary
import vulcan.Codec
import vulcan.generic._

sealed trait SealedTraitCaseClassAvroNamespace

final case class FirstInSealedTraitCaseClassAvroNamespace(value: Int)
    extends SealedTraitCaseClassAvroNamespace

@AvroNamespace("com.example")
final case class SecondInSealedTraitCaseClassAvroNamespace(value: String)
    extends SealedTraitCaseClassAvroNamespace

object SealedTraitCaseClassAvroNamespace {
  implicit val sealedTraitCaseClassAvroNamespaceArbitrary
    : Arbitrary[SealedTraitCaseClassAvroNamespace] =
    Arbitrary(
      Gen.oneOf(
        arbitrary[Int].map(FirstInSealedTraitCaseClassAvroNamespace(_)),
        arbitrary[String].map(SecondInSealedTraitCaseClassAvroNamespace(_))
      )
    )

  implicit val sealedTraitCaseClassAvroNamespaceEq: Eq[SealedTraitCaseClassAvroNamespace] =
    Eq.fromUniversalEquals

  implicit val codec: Codec[SealedTraitCaseClassAvroNamespace] =
    Codec.derive
}
