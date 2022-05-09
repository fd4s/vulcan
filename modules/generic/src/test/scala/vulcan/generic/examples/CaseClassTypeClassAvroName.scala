package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

@AvroName("CaseClassOtherName")
final case class CaseClassTypeClassAvroName[A](value: Option[A])

object CaseClassTypeClassAvroName {
  implicit val codecString: Codec[CaseClassTypeClassAvroName[String]] = Codec.derive
}
