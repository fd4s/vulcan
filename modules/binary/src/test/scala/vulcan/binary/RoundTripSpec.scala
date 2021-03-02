package vulcan.binary

import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.util.Utf8
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalacheck.Arbitrary
import org.scalatest.Assertion
import scodec.DecodeResult
import scodec.bits.BitVector
import org.scalacheck.Arbitrary.arbitrary
import vulcan.Codec
import vulcan.internal.converters.collection._
import vulcan.binary.examples.{CaseClassThreeFields, SealedTraitEnum}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class RoundTripSpec extends BaseSpec with RoundTripHelpers {
  implicit val arbFloat: Arbitrary[java.lang.Float] = Arbitrary(
    implicitly[Arbitrary[Float]].arbitrary.map(java.lang.Float.valueOf(_))
  )

  implicit val arbDouble: Arbitrary[java.lang.Double] = Arbitrary(
    implicitly[Arbitrary[Double]].arbitrary.map(java.lang.Double.valueOf(_))
  )

  implicit val arbInteger: Arbitrary[java.lang.Integer] = Arbitrary(
    implicitly[Arbitrary[Int]].arbitrary.map(java.lang.Integer.valueOf(_))
  )

  implicit val arbLong: Arbitrary[java.lang.Long] = Arbitrary(
    implicitly[Arbitrary[Long]].arbitrary.map(java.lang.Long.valueOf(_))
  )

  implicit val arbUtf8: Arbitrary[Utf8] = Arbitrary(
    implicitly[Arbitrary[String]].arbitrary.map(new Utf8(_))
  )

  implicit val arbBool: Arbitrary[java.lang.Boolean] = Arbitrary(
    implicitly[Arbitrary[Boolean]].arbitrary.map(java.lang.Boolean.valueOf(_))
  )

  implicit val arbRecord: Arbitrary[GenericRecord] = Arbitrary(
    arbitrary[CaseClassThreeFields]
      .map(Codec.encode(_).value.asInstanceOf[GenericRecord])
  )

  implicit def arbList[A: Arbitrary]: Arbitrary[java.util.List[A]] = Arbitrary(
    arbitrary[List[A]].map(_.asJava)
  )

  implicit val arbEnumSymbol: Arbitrary[GenericData.EnumSymbol] = Arbitrary(
    arbitrary[SealedTraitEnum].map(entry => SealedTraitEnum.codec.encode(entry).value)
  )

  describe("float") {
    it("roundtrip") {
      roundtrip[java.lang.Float](SchemaBuilder.builder().floatType())
    }
  }

  describe("double") {
    it("roundtrip") {
      roundtrip[java.lang.Double](SchemaBuilder.builder().doubleType())
    }
  }

  describe("int") {
    it("roundtrip") {
      roundtrip[java.lang.Integer](SchemaBuilder.builder().intType())
    }
  }

  describe("long") {
    it("roundtrip") {
      roundtrip[java.lang.Long](SchemaBuilder.builder().longType())
    }
  }

  describe("string") {
    it("roundtrip") {
      roundtrip[Utf8](SchemaBuilder.builder().stringType())
    }
  }

  describe("boolean") {
    it("roundtrip") {
      roundtrip[java.lang.Boolean](SchemaBuilder.builder().booleanType())
    }
  }

  describe("array") {
    it("roundtrip") {
      roundtrip[java.util.List[java.lang.Long]](
        SchemaBuilder.builder().array().items(SchemaBuilder.builder().longType())
      )
    }
  }

  describe("null") {
    it("roundtrip") {
      scodecThenJava(null, SchemaBuilder.builder().nullType)
      javaThenScodec(null, SchemaBuilder.builder().nullType)
    }
  }

  describe("Record") {
    it("roundtrip") { roundtrip[GenericRecord](Codec[CaseClassThreeFields].schema.value) }
  }

  describe("Enum") {
    it("roundtrip") {
      roundtrip[GenericData.EnumSymbol](Codec[SealedTraitEnum].schema.value)
    }
  }
}

trait RoundTripHelpers {
  self: BaseSpec =>

  def roundtrip[A](schema: Schema)(
    implicit arbitrary: Arbitrary[A]
  ): Assertion = {
    forAll { (a: A) =>
      scodecThenScodec(a, schema)
      scodecThenJava(a, schema)
      javaThenScodec(a, schema)
    }
  }

  def scodecThenScodec(value: Any, schema: Schema) = {
    val encoded = forWriterSchema(schema).encode(value).fold(err => fail(err.toString), identity)
    val result = forWriterSchema(schema).decode(encoded).fold(err => fail(err.toString), identity)

    assert(result === DecodeResult(value, BitVector.empty))
  }

  def scodecThenJava(value: Any, schema: Schema) = {
    val encoded = forWriterSchema(schema).encode(value).fold(err => fail(err.toString), identity)
    val bais = new ByteArrayInputStream(encoded.toByteArray)
    val decoder = DecoderFactory.get.binaryDecoder(bais, null)
    val result = new GenericDatumReader[Any](schema).read(null, decoder)

    assert(result === value)
  }

  def javaThenScodec(value: Any, schema: Schema) = {
    val baos = new ByteArrayOutputStream
    val encoder = EncoderFactory.get.binaryEncoder(baos, null)
    new GenericDatumWriter[Any](schema).write(value, encoder)
    encoder.flush()
    val encoded = baos.toByteArray

    val result = forWriterSchema(schema).decode(BitVector(encoded)).getOrElse(fail())
    assert(result === DecodeResult(value, BitVector.empty))
  }
}
