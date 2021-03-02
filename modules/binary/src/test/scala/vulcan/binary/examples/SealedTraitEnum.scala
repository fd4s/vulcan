package vulcan.binary.examples

import cats.Eq
import org.scalacheck.{Arbitrary, Gen}
import vulcan.{AvroError, Codec, Props}
import org.apache.avro.generic.GenericData

sealed trait SealedTraitEnum extends Product with Serializable

object SealedTraitEnum {
  implicit final val arbitrary: Arbitrary[SealedTraitEnum] =
    Arbitrary(Gen.oneOf(FirstInSealedTraitEnum, SecondInSealedTraitEnum))

  implicit final val eq: Eq[SealedTraitEnum] =
    Eq.fromUniversalEquals

  implicit final val codec: Codec.Aux[ GenericData.EnumSymbol, SealedTraitEnum] =
    Codec.enumeration(
      name = "SealedTraitEnum",
      namespace = "vulcan.examples",
      symbols = List("first", "second"),
      aliases = List("first", "second"),
      doc = Some("documentation"),
      default = Some(FirstInSealedTraitEnum),
      encode = (e: SealedTraitEnum) => (e match {
        case FirstInSealedTraitEnum  => "first"
        case SecondInSealedTraitEnum => "second"
      }),
      decode = {
        case "first"  => Right(FirstInSealedTraitEnum)
        case "second" => Right(SecondInSealedTraitEnum)
        case other    => Left(AvroError(other))
      },
      props = Props.one("custom1", 10).add("custom2", "value2")
    )
}

case object FirstInSealedTraitEnum extends SealedTraitEnum

case object SecondInSealedTraitEnum extends SealedTraitEnum
