package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

final case class CaseClassFieldAvroDoc(@AvroDoc("documentation") name: String)

object CaseClassFieldAvroDoc {
  implicit val codec: Codec[CaseClassFieldAvroDoc] =
    Codec.derive
}
