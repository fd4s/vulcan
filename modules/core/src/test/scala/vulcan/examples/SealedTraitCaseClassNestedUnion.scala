package vulcan.examples

import vulcan.Codec

sealed trait SealedTraitCaseClassNestedUnion

object SealedTraitCaseClassNestedUnion {
  implicit def codec: Codec[SealedTraitCaseClassNestedUnion] =
    Codec.union { alt =>
      alt[NestedUnionInSealedTraitCaseClassNestedUnion]
    }
}

final case class NestedUnionInSealedTraitCaseClassNestedUnion(value: Option[Int])
    extends SealedTraitCaseClassNestedUnion

object NestedUnionInSealedTraitCaseClassNestedUnion {
  implicit val codec: Codec[NestedUnionInSealedTraitCaseClassNestedUnion] =
    Codec[Option[Int]].imap(apply)(_.value)
}
