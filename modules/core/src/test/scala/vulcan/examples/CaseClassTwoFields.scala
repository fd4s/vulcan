package vulcan.examples

import cats.implicits._
import org.apache.avro.Schema
import vulcan.Codec

final case class CaseClassTwoFields(name: String, age: Int)

object CaseClassTwoFields {
  implicit val codec: Codec[CaseClassTwoFields] =
    Codec.record(
      name = "CaseClassTwoFields",
      namespace = Some("vulcan.examples"),
      doc = Some("some documentation for example"),
      aliases = Seq("FirstAlias", "SecondAlias"),
      props = Seq("custom" -> "custom record value")
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
          props = Seq("custom" -> "custom field value")
        ),
        field("age", _.age)
      ).mapN(CaseClassTwoFields(_, _))
    }
}
