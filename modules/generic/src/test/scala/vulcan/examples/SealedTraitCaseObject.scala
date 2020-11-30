package vulcan.examples

import vulcan.Codec
import vulcan.generic._

sealed trait SealedTraitCaseObject

case object CaseObjectInSealedTrait extends SealedTraitCaseObject

object SealedTraitCaseObject {
  implicit val codec: Codec[SealedTraitCaseObject] =
    Codec.derive
}
