package vulcan.examples

import vulcan.{AvroDoc, Codec}

@AvroDoc("documentation")
final case class CaseClassAvroDoc(value: Option[String])

object CaseClassAvroDoc {
  implicit val codec: Codec[CaseClassAvroDoc] =
    Codec.derive
}
