package vulcan.examples

import cats.Eq
import org.scalacheck.{Arbitrary, Gen}
import vulcan.{AvroError, AvroNamespace, Codec}

@AvroNamespace("com.example")
sealed trait SealedTraitEnumDerived

object SealedTraitEnumDerived {
  implicit final val arbitrary: Arbitrary[SealedTraitEnumDerived] =
    Arbitrary(Gen.oneOf(FirstInSealedTraitEnumDerived, SecondInSealedTraitEnumDerived))

  implicit final val eq: Eq[SealedTraitEnumDerived] =
    Eq.fromUniversalEquals

  implicit final val codec: Codec[SealedTraitEnumDerived] =
    Codec.deriveEnum(
      symbols = List("first", "second"),
      encode = {
        case FirstInSealedTraitEnumDerived  => "first"
        case SecondInSealedTraitEnumDerived => "second"
      },
      decode = {
        case "first"  => Right(FirstInSealedTraitEnumDerived)
        case "second" => Right(SecondInSealedTraitEnumDerived)
        case other    => Left(AvroError(other))
      }
    )
}

case object FirstInSealedTraitEnumDerived extends SealedTraitEnumDerived

case object SecondInSealedTraitEnumDerived extends SealedTraitEnumDerived
