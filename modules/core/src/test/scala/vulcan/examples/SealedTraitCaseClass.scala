package vulcan.examples

import vulcan.Codec

sealed trait SealedTraitCaseClass

final case class CaseClassInSealedTrait(value: Int) extends SealedTraitCaseClass

object SealedTraitCaseClass {
  implicit val codec: Codec[SealedTraitCaseClass] =
    Codec.derive
}
