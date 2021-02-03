package vulcan.scodec.examples

import cats.Eq
import cats.implicits._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{Arbitrary, Gen}
import vulcan.Codec

sealed trait SealedTraitCaseClass

object SealedTraitCaseClass {
  implicit val sealedTraitCaseClassCodec: Codec[SealedTraitCaseClass] =
    Codec.union { alt =>
      assert(alt.toString() == "AltBuilder")

      alt[FirstInSealedTraitCaseClass] |+|
        alt[SecondInSealedTraitCaseClass] |+|
        alt[ThirdInSealedTraitCaseClass]
    }

  implicit val sealedTraitCaseClassEq: Eq[SealedTraitCaseClass] =
    Eq.fromUniversalEquals

  implicit val sealedTraitCaseClassArbitrary: Arbitrary[SealedTraitCaseClass] =
    Arbitrary {
      Gen.oneOf[SealedTraitCaseClass](
        arbitrary[Int].map(FirstInSealedTraitCaseClass(_)),
        arbitrary[String].map(SecondInSealedTraitCaseClass(_)),
        arbitrary[Int].map(ThirdInSealedTraitCaseClass(_))
      )
    }
}

final case class FirstInSealedTraitCaseClass(value: Int) extends SealedTraitCaseClass

object FirstInSealedTraitCaseClass {
  implicit val codec: Codec[FirstInSealedTraitCaseClass] =
    Codec.record(
      name = "FirstInSealedTraitCaseClass",
      namespace = "com.example"
    ) { field =>
      field("value", _.value).map(apply)
    }

  implicit val arb: Arbitrary[FirstInSealedTraitCaseClass] =
    Arbitrary(arbitrary[Int].map(apply))
}

final case class SecondInSealedTraitCaseClass(value: String) extends SealedTraitCaseClass

object SecondInSealedTraitCaseClass {
  implicit val codec: Codec[SecondInSealedTraitCaseClass] =
    Codec.record(
      name = "SecondInSealedTraitCaseClass",
      namespace = "com.example"
    ) { field =>
      field("value", _.value).map(apply)
    }

  implicit val arb: Arbitrary[SecondInSealedTraitCaseClass] =
    Arbitrary(arbitrary[String].map(apply))
}

final case class ThirdInSealedTraitCaseClass(value: Int) extends SealedTraitCaseClass

object ThirdInSealedTraitCaseClass {
  implicit val codec: Codec[ThirdInSealedTraitCaseClass] =
    Codec[Int].imap(apply)(_.value)

  implicit val arb: Arbitrary[ThirdInSealedTraitCaseClass] =
    Arbitrary(arbitrary[Int].map(apply))
}
