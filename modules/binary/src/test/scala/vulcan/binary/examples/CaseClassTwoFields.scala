package vulcan.binary.examples

import cats.implicits._
import org.apache.avro.Schema
import vulcan.{Codec, Props}

final case class CaseClassTwoFields(name: String, bar: Double, age: Int)

object CaseClassTwoFields {
  implicit val codec: Codec[CaseClassTwoFields] =
    Codec.record(
      name = "CaseClass",
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
        field("bar", _.bar, default = Some(1.2)),
        field("age", _.age)
      ).mapN(CaseClassTwoFields(_, _, _))
    }
}
