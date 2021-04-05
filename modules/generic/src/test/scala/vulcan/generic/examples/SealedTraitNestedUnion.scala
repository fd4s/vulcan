package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

sealed trait SealedTraitNestedUnion

object SealedTraitNestedUnion {
  implicit def codec: Codec[SealedTraitNestedUnion] =
    Codec.derive
}

final case class NestedUnionInSealedTrait(value: Option[Int]) extends SealedTraitNestedUnion

object NestedUnionInSealedTrait {
  implicit final val codec: Codec[NestedUnionInSealedTrait] =
    Codec.option[Int].imap(apply)(_.value)
}
