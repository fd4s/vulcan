package vulcan.generic.examples

import vulcan.{AvroError, Codec}
import vulcan.generic._

object AvroRecordDefault {
  sealed trait Enum extends Product {
    self =>
    def value: String = self.productPrefix
  }

  object Enum {
    case object A extends Enum

    case object B extends Enum

    implicit val codec: Codec[Enum] = deriveEnum(
      symbols = List(A.value, B.value),
      encode = _.value,
      decode = {
        case "A"   => Right(A)
        case "B"   => Right(B)
        case other => Left(AvroError(s"Invalid S: $other"))
      }
    )
  }

  sealed trait Union

  object Union {
    case class A(a: Int) extends Union

    case class B(b: String) extends Union

    implicit val codec: Codec[Union] = Codec.derive
  }

  case class Foo(
                  a: Int = 1,
                  b: String = "foo",
                  c: Option[String] = None
                )

  object Foo {
    implicit val codec: Codec[Foo] = Codec.derive
  }

  case class InvalidDefault2(
                              a: Option[String] = Some("foo")
                            )
  object InvalidDefault2 {
    implicit val codec: Codec[InvalidDefault2] = Codec.derive
  }

  case class HasSFirst(
                        s: Enum = Enum.A
                      )
  object HasSFirst {
    implicit val codec: Codec[HasSFirst] = Codec.derive
  }

  case class HasSSecond(
                         s: Enum = Enum.B
                       )
  object HasSSecond {
    implicit val codec: Codec[HasSSecond] = Codec.derive
  }

  case class HasUnion(
                       u: Union = Union.A(1)
                     )
  object HasUnion {
    implicit val codec: Codec[HasUnion] = Codec.derive
  }

  case class Empty()
  object Empty {
    implicit val codec: Codec[Empty] = Codec.derive
  }

  case class HasUnionSecond(
                             u: Union = Union.B("foo")
                           )
  object HasUnionSecond {
    implicit val codec: Codec[HasUnionSecond] = Codec.derive
  }
}


