package vulcan.generic.examples

import cats.Eq
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Arbitrary.arbitrary
import vulcan.Codec
import vulcan.generic._
import A._
import B._

sealed trait SealedTraitCaseClassAvroNamespace

object A {
@AvroNamespace("com.example")
final case class SecondInSealedTraitCaseClassAvroNamespace(value: String)
    extends SealedTraitCaseClassAvroNamespace
}
object B {
final case class FirstInSealedTraitCaseClassAvroNamespace(value: Int)
    extends SealedTraitCaseClassAvroNamespace
}

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
