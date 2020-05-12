package vulcan.examples

import vulcan.Codec
import vulcan.generic._

sealed trait SealedTraitCaseClass

final case class CaseClassInSealedTrait(value: Int) extends SealedTraitCaseClass

object CaseClassInSealedTrait {
  implicit val codec: Codec[CaseClassInSealedTrait] =
    Codec.derive
}

object SealedTraitCaseClass {
  implicit val codec: Codec[SealedTraitCaseClass] =
    Codec.derive
}
