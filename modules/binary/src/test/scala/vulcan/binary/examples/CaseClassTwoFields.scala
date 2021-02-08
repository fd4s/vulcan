package vulcan.binary.examples

import cats.implicits._
import org.apache.avro.Schema
import vulcan.{Codec, Props}

final case class CaseClassTwoFields(name: String, age: Int)

object CaseClassTwoFields {
  implicit val codec: Codec[CaseClassTwoFields] =
    Codec.record(
      name = "CaseClassTwoFields",
      namespace = "vulcan.examples",
      doc = Some("some documentation for example"),
      aliases = Seq("FirstAlias", "SecondAlias"),
      props = Props.one("custom", List(1, 2, 3))
    ) { field =>
      assert(field.toString() == "FieldBuilder")

      (
        field(
          name = "name",
          access = _.name,
          doc = Some("some doc"),
          default = Some("default name"),
          order = Some(Schema.Field.Order.DESCENDING),
          aliases = Seq("TheAlias"),
          props = Props.one("custom", "value")
        ),
        field("age", _.age)
      ).mapN(CaseClassTwoFields(_, _))
    }
}
