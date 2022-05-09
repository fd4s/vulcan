package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

final case class CaseClassTypeClass[A](value: Option[A])

object CaseClassTypeClass {
  implicit val codecString: Codec[CaseClassTypeClass[String]] = Codec.derive
}
