package vulcan.generic.examples

import vulcan.Codec

final case class CaseClassValueClass(value: Int) extends AnyVal

object CaseClassValueClass {
  implicit val codec: Codec[CaseClassValueClass] =
    // we currently don't autoderive codecs for value classes in Scala 3 
    Codec.int.imap(apply)(_.value)
}
