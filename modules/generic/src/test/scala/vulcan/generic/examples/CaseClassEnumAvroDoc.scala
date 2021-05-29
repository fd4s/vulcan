package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic.{AvroDoc, deriveEnum}

@AvroDoc("documentation")
final case class CaseClassEnumAvroDoc(value: Option[String])

object CaseClassEnumAvroDoc {
  implicit val codec: Codec[CaseClassEnumAvroDoc] =
    deriveEnum(
      symbols = List("first"),
      encode = _ => "first",
      decode = _ => Right(CaseClassEnumAvroDoc(None))
    )
}
