package vulcan.binary

import org.apache.avro.SchemaBuilder
import org.scalatest.matchers.should.Matchers
import vulcan.Codec
import vulcan.binary.examples.{CaseClassThreeFields, CaseClassTwoFields}
import vulcan.binary.internal.ArrayScodec
import vulcan.internal.converters.collection._

class ResolutionSpec extends BaseSpec with Matchers {
  describe("long") {
    it("should decode int") {
      decode[Long](intScodec.encode(42).require.toByteArray, SchemaBuilder.builder().intType()).value should ===(
        42L
      )
    }
  }

  describe("array") {
    it("should recursively resolve") {
      decode[List[Long]](
        ArrayScodec(intScodec).encode(List(1, 2, 3).asJava).require.toByteArray,
        SchemaBuilder.array().items(SchemaBuilder.builder().intType())
      ).value should ===(
        List(1L, 2L, 3L)
      )
    }
  }

  describe("record") {
    it("should decode record with more fields") {
      val encodedThreeFields = encode(CaseClassThreeFields("bob", 23, 2.3)).value

      decode[CaseClassTwoFields](encodedThreeFields, CaseClassThreeFields.codec.schema.value).value should ===(
        CaseClassTwoFields("bob", 1.2, 23)
      )
    }

    it("should fail to decode record with missing fields") {
      val encodedTwoFields = encode(CaseClassTwoFields("bob", 12.32, 23)).value

      decode[CaseClassThreeFields](encodedTwoFields, CaseClassTwoFields.codec.schema.value).swap.value.message should ===(
        "missing field foo with no default"
      )
    }
  }

  describe("enum") {
    it("should decode overlapping enums") {
      val writer =
        Codec.enumeration[String]("foo", "a", List("a", "b", "c", "d"), identity, s => Right(s))

      val reader =
        Codec.enumeration[String](
          "foo",
          "a",
          List("n1", "n2", "a", "b"),
          identity,
          s => Right(s),
          default = Some("n2")
        )

      val encoded = encode("a")(writer).value
      val decoded = decode(encoded, writer.schema.value)(reader)

      decoded.value should ===("a")
    }

    it("should fall back to default overlapping enums") {
      val writer =
        Codec.enumeration[String]("foo", "a", List("a", "b", "c", "d"), identity, s => Right(s))

      val reader =
        Codec.enumeration[String](
          "foo",
          "a",
          List("n1", "n2", "a", "b"),
          identity,
          s => Right(s),
          default = Some("n2")
        )

      val encoded = encode("c")(writer).value
      val decoded = decode(encoded, writer.schema.value)(reader)

      decoded.value should ===("n2")
    }
  }

  describe("union") {}

  describe("int") {
    it("decoding long should error") {
      decode[Int](longScodec.encode(42L).require.toByteArray, SchemaBuilder.builder().longType()).swap.value.message should ===(
        "Reader schema of type INT cannot read output of writer schema of type LONG"
      )
    }
  }
}
