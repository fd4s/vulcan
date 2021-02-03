package vulcan.scodec.examples

import vulcan.Codec

sealed trait SealedTraitCaseClassIncomplete

object SealedTraitCaseClassIncomplete {
  implicit val codec: Codec[SealedTraitCaseClassIncomplete] =
    Codec.union { alt =>
      alt[FirstInSealedTraitCaseClassIncomplete]
    }
}

final case class FirstInSealedTraitCaseClassIncomplete(value: Int)
    extends SealedTraitCaseClassIncomplete

object FirstInSealedTraitCaseClassIncomplete {
  implicit val codec: Codec[FirstInSealedTraitCaseClassIncomplete] =
    Codec[Int].imap(apply)(_.value)
}

final case class SecondInSealedTraitCaseClassIncomplete(value: Double)
    extends SealedTraitCaseClassIncomplete

object SecondInSealedTraitCaseClassIncomplete {
  implicit val codec: Codec[SecondInSealedTraitCaseClassIncomplete] =
    Codec[Double].imap(apply)(_.value)
}
