package vulcan.examples

import vulcan.Codec
import vulcan.generic._

sealed trait SealedTraitCaseClass

final case class CaseClassInSealedTrait(value: Int) extends SealedTraitCaseClass

object SealedTraitCaseClass {
  implicit val codec: Codec[SealedTraitCaseClass] =
    Codec.derive
}
