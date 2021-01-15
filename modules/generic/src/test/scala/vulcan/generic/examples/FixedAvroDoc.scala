package vulcan.generic.examples

import vulcan._
import vulcan.generic._

@AvroDoc("Some documentation")
final case class FixedAvroDoc(bytes: Array[Byte])

object FixedAvroDoc {
  implicit val codec: Codec[FixedAvroDoc] =
    deriveFixed(
      size = 1,
      encode = _.bytes,
      decode = bytes => Right(apply(bytes))
    )
}
