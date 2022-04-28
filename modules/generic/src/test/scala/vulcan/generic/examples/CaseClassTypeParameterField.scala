package vulcan.generic.examples

import vulcan.Codec
import vulcan.generic._

final case class CaseClassTypeParameterField[T](s: String, value: T)
final case class CaseClassInner[T](inner: T)

object CaseClassTypeParameterField {
  implicit val intCodec: Codec[CaseClassTypeParameterField[Int]] =
    Codec.derive

  implicit val longCodec: Codec[CaseClassTypeParameterField[Long]] =
    Codec.derive

  implicit val innerIntCodec: Codec[CaseClassInner[Int]] =
    Codec.derive

  implicit val withInnerIntCodec: Codec[CaseClassTypeParameterField[CaseClassInner[Int]]] =
    Codec.derive
}
