package vulcan.examples

import cats.Eq
import cats.implicits._
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Arbitrary.arbitrary
import vulcan.Codec

sealed trait SealedTraitCaseClassSharedName

object SealedTraitCaseClassSharedName {
  implicit val sealedTraitCaseClassCodec: Codec[SealedTraitCaseClassSharedName] =
    Codec.union { alt =>
      assert(alt.toString() == "AltBuilder")

      alt[First.SharedNameSealedTraitCaseClass] |+| alt[Second.SharedNameSealedTraitCaseClass]
    }

  implicit val sealedTraitCaseClassEq: Eq[SealedTraitCaseClassSharedName] =
    Eq.fromUniversalEquals

  implicit val sealedTraitCaseClassArbitrary: Arbitrary[SealedTraitCaseClassSharedName] =
    Arbitrary {
      Gen.oneOf[SealedTraitCaseClassSharedName](
        arbitrary[Int].map(First.SharedNameSealedTraitCaseClass(_)),
        arbitrary[String].map(Second.SharedNameSealedTraitCaseClass(_))
      )
    }
}

object First {
  final case class SharedNameSealedTraitCaseClass(value: Int) extends SealedTraitCaseClassSharedName

  object SharedNameSealedTraitCaseClass {
    implicit val codec: Codec[SharedNameSealedTraitCaseClass] =
      Codec.record(
        name = "SharedNameSealedTraitCaseClass",
        namespace = "com.example.first"
      ) { field =>
        field("value", _.value).map(apply)
      }

    implicit val arb: Arbitrary[SharedNameSealedTraitCaseClass] =
      Arbitrary(arbitrary[Int].map(apply))
  }

}

object Second {
  final case class SharedNameSealedTraitCaseClass(value: String)
      extends SealedTraitCaseClassSharedName

  object SharedNameSealedTraitCaseClass {
    implicit val codec: Codec[SharedNameSealedTraitCaseClass] =
      Codec.record(
        name = "SharedNameSealedTraitCaseClass",
        namespace = "com.example.second"
      ) { field =>
        field("value", _.value).map(apply)
      }

    implicit val arb: Arbitrary[SharedNameSealedTraitCaseClass] =
      Arbitrary(arbitrary[String].map(apply))

  }

}
