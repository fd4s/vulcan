package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

final case class CaseClassValueClass(value: Int) extends AnyVal

object CaseClassValueClass {
  implicit val codec: Codec[CaseClassValueClass] = Codec.derive
}
