package vulcan.examples

import vulcan.{AvroError, Codec}

sealed trait `-SealedTraitEnumInvalidName`

object `-SealedTraitEnumInvalidName` {
  implicit def codec: Codec[`-SealedTraitEnumInvalidName`] =
    Codec.enumeration(
      "-SealedTraitEnumInvalidName",
      symbols = Seq("value"),
      encode = _ => "value",
      decode = _ => Left(AvroError("error")),
      namespace = ""
    )
}

case object FirstInSealedTraitEnumInvalidName extends `-SealedTraitEnumInvalidName`

case object SecondInSealedTraitEnumInvalidName extends `-SealedTraitEnumInvalidName`
