package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

@AvroName("FixedOtherName")
final case class FixedAvroName(bytes: Array[Byte])

object FixedAvroName {
  implicit val codec: Codec[FixedAvroName] =
    deriveFixed(
      size = 1,
      encode = _.bytes,
      decode = bytes => Right(apply(bytes))
    )
}
