package vulcan.examples

import vulcan.{AvroError, Codec}

sealed trait `-SealedTraitEnumInvalidName`

object `-SealedTraitEnumInvalidName` {
  implicit val codec: Codec[`-SealedTraitEnumInvalidName`] =
    Codec.enum(
      "-SealedTraitEnumInvalidName",
      symbols = Seq("value"),
      encode = _ => "value",
      decode = _ => Left(AvroError("error")),
      namespace = ""
    )
}

final case object FirstInSealedTraitEnumInvalidName extends `-SealedTraitEnumInvalidName`

final case object SecondInSealedTraitEnumInvalidName extends `-SealedTraitEnumInvalidName`
