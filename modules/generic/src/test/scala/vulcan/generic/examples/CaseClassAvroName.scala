package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

@AvroName("CaseClassOtherName")
final case class CaseClassAvroName(
  @AvroName("otherValue")
  value: Option[String]
)

object CaseClassAvroName {
  implicit val codec: Codec[CaseClassAvroName] = Codec.derive
}
