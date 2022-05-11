package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

case class CaseClassOptionOfSumType(field1: Option[SealedTraitCaseClassCustom])

object CaseClassOptionOfSumType {
  implicit val codec: Codec[CaseClassOptionOfSumType] = Codec.derive
}
