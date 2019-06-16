package vulcan.examples

import vulcan.Codec

final case class CaseClassFieldInvalidName(`-value`: Int)

object CaseClassFieldInvalidName {
  implicit val codec: Codec[CaseClassFieldInvalidName] =
    Codec.derive
}
