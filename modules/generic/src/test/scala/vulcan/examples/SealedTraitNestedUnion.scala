package vulcan.examples

import vulcan.Codec
import vulcan.generic._

sealed trait SealedTraitNestedUnion

object SealedTraitNestedUnion {
  implicit val codec: Codec[SealedTraitNestedUnion] =
    Codec.derive
}

final case class NestedUnionInSealedTrait(value: Option[Int]) extends SealedTraitNestedUnion

final object NestedUnionInSealedTrait {
  implicit final val codec: Codec[NestedUnionInSealedTrait] =
    Codec.option[Int].imap(apply)(_.value)
}
