package vulcan.examples

import vulcan.Codec

final case class CaseClassValueClass(value: Int) extends AnyVal

object CaseClassValueClass {
  implicit val codec: Codec[CaseClassValueClass] =
    Codec.derive
}
