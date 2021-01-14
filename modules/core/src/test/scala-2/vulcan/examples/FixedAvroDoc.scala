package vulcan.examples

import scala.annotation.nowarn
import vulcan.{AvroDoc, Codec}

@AvroDoc("Some documentation")
final case class FixedAvroDoc(bytes: Array[Byte])

object FixedAvroDoc {
  @nowarn("msg=deprecated")
  implicit val codec: Codec[FixedAvroDoc] =
    Codec.deriveFixed(
      size = 1,
      encode = _.bytes,
      decode = bytes => Right(apply(bytes))
    )
}
