package vulcan.examples

import vulcan.Codec

sealed trait SealedTraitCaseClassSingle

object SealedTraitCaseClassSingle {
  implicit val codec: Codec[SealedTraitCaseClassSingle] =
    Codec.union { alt =>
      alt[CaseClassInSealedTraitCaseClassSingle]
    }
}

final case class CaseClassInSealedTraitCaseClassSingle(value: Int)
    extends SealedTraitCaseClassSingle

object CaseClassInSealedTraitCaseClassSingle {
  implicit val codec: Codec[CaseClassInSealedTraitCaseClassSingle] =
    Codec.record(
      name = "CaseClassInSealedTraitCaseClassSingle",
      namespace = "vulcan.examples"
    ) { field =>
      field("value", _.value).map(apply)
    }
}
