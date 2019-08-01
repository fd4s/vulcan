package vulcan.examples

import vulcan.{AvroDoc, Codec}

@AvroDoc("Some documentation")
final case class FixedAvroDoc(bytes: Array[Byte])

object FixedAvroDoc {
  implicit val codec: Codec[FixedAvroDoc] =
    Codec.deriveFixed(
      size = 1,
      encode = _.bytes,
      decode = bytes => Right(apply(bytes))
    )
}
