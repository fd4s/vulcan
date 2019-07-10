package vulcan.examples

import vulcan.Codec
import vulcan.generic._

final case class CaseClassTwoFields(name: String, age: Int)

object CaseClassTwoFields {
  implicit val codec: Codec[CaseClassTwoFields] =
    Codec.derive
}
