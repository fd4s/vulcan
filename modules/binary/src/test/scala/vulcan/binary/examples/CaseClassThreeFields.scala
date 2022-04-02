package vulcan.binary.examples

import cats.implicits._
import org.apache.avro.Schema
import org.scalacheck.Arbitrary
import vulcan.{Codec, Props}
import org.scalacheck.Arbitrary.arbitrary

final case class CaseClassThreeFields(name: String, age: Int, foo: Double)

object CaseClassThreeFields {
  implicit val codec: Codec[CaseClassThreeFields] =
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
          default = Some("hi"),
          order = Some(Schema.Field.Order.DESCENDING),
          aliases = Seq("TheAlias"),
          props = Props.one("custom", "value")
        ),
        field("age", _.age),
        field("foo", _.foo)
      ).mapN(CaseClassThreeFields(_, _, _))
    }

  implicit val arb: Arbitrary[CaseClassThreeFields] = Arbitrary {
    for {
      name <- arbitrary[String]
      age <- arbitrary[Int]
      foo <- arbitrary[Double]
    } yield CaseClassThreeFields(name, age, foo)
  }
}
