package vulcan.examples

import vulcan.{AvroDoc, Codec}

final case class CaseClassFieldAvroDoc(@AvroDoc("documentation") name: String)

object CaseClassFieldAvroDoc {
  implicit val codec: Codec[CaseClassFieldAvroDoc] =
    Codec.derive
}
