package vulcan.fuuid

import cats.effect.IO
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import io.chrisdavenport.fuuid.FUUID
import org.apache.avro.util.Utf8
import org.scalatest.EitherValues
import vulcan.Codec

import java.util.UUID

class FuuidSpec extends AnyFunSpec with ScalaCheckPropertyChecks with EitherValues {
  describe("FUUID") {
    it("should roundtrip") {
      val fuuid = FUUID.randomFUUID[IO].unsafeRunSync()
      val codec = Codec[FUUID]
      val schema = codec.schema.value
      val encoded = codec.encode(fuuid).value
      val decoded = codec.decode(encoded, schema).value
      assert(decoded === fuuid)
    }

    describe("schema") {
      it("should be encoded as string with logical type uuid") {
        assert(Codec[FUUID].schema.value.toString == """{"type":"string","logicalType":"uuid"}""")
      }
    }

    describe("encode") {
      it("should encode as utf8") {
        val fuuid = FUUID.randomFUUID[IO].unsafeRunSync()

        assert {
          val encode = Codec[FUUID].encode(fuuid).value
          encode === new Utf8(fuuid.toString())
        }
      }
    }

    describe("decode") {
      it("should error if schema is not string") {
        val value = Codec[FUUID].encode(FUUID.randomFUUID[IO].unsafeRunSync()).value
        val schema = Codec[Int].schema.value
        assert(
          Codec[FUUID]
            .decode(value, schema)
            .swap
            .value
            .message == "Got unexpected schema type INT while decoding UUID, expected schema type STRING"
        )
      }

      it("should error if logical type is not uuid") {
        val value = Codec[FUUID].encode(FUUID.randomFUUID[IO].unsafeRunSync()).value
        val schema = Codec[String].schema.value
        assert(
          Codec[FUUID]
            .decode(value, schema)
            .swap
            .value
            .message == "Got unexpected missing logical type while decoding UUID"
        )
      }

      it("should error if value is not utf8") {
        val value = 10
        val schema = Codec[UUID].schema.value
        assert(
          Codec[FUUID]
            .decode(value, schema)
            .swap
            .value
            .message == "Got unexpected type java.lang.Integer while decoding UUID, expected type Utf8"
        )
      }

      it("should error if value is not uuid") {
        val value = new Utf8("not-uuid")
        val schema = Codec[UUID].schema.value
        assert(
          Codec[FUUID]
            .decode(value, schema)
            .swap
            .value
            .message == "java.lang.IllegalArgumentException: Invalid UUID string: not-uuid"
        )
      }
    }
  }
}
