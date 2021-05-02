package vulcan
import cats.syntax.all._
import org.apache.avro.generic.{GenericData, GenericDatumReader}
import org.apache.avro.io.DecoderFactory

import java.io.ByteArrayInputStream
import java.time.Instant

class EndToEndResolutionSpec extends BaseSpec {
  describe("Instant") {
    it("should fail to decode plain Long") {
      assert(binaryRoundtrip[Long, Instant](123L).isLeft)
    }
  }
  describe("record with Instant") {
    final case class InstantRecord(instant: Instant)
    implicit val iCodec: Codec[InstantRecord] = Codec.record("example", "") { field =>
      field[Instant]("instant", _.instant).map(InstantRecord(_))
    }

    final case class LongRecord(instant: Long)
    implicit val lCodec: Codec[LongRecord] = Codec.record("example", "") { field =>
      field[Long]("instant", _.instant).map(LongRecord(_))
    }

    it("should fail to decode record with plain Long") {
      val result = binaryRoundtrip[LongRecord, InstantRecord](LongRecord(123L))
      assert(result.isLeft)
    }
  }

  describe("record with different BigDecimal scale") {
    final case class BigDecimalHigh(bd: BigDecimal)
    implicit val hCodec: Codec[BigDecimalHigh] = Codec.record("example", "") { field =>
      field[BigDecimal]("bd", _.bd)(Codec.decimal(4, 2)).map(BigDecimalHigh(_))
    }

    final case class BigDecimalLow(bd: BigDecimal)
    implicit val lCodec: Codec[BigDecimalLow] = Codec.record("example", "") { field =>
      field[BigDecimal]("bd", _.bd)(Codec.decimal(4, 1)).map(BigDecimalLow(_))
    }

    it("should decode to same value") {
      val result =
        binaryRoundtrip[BigDecimalHigh, BigDecimalLow](BigDecimalHigh(BigDecimal("10.00")))
      assert(result.value.bd == BigDecimal("10.00"))
    }
  }

  def binaryRoundtrip[From: Codec, To: Codec](from: From): Either[AvroError, To] =
    (Codec[From].schema, Codec[To].schema, Codec.toBinary(from)).tupled.flatMap {
      case (writerSchema, readerSchema, bytes) =>
        val bais = new ByteArrayInputStream(bytes)
        val deserializer = DecoderFactory.get().binaryDecoder(bais, null)
        val read =
          new GenericDatumReader[Any](
            writerSchema,
            readerSchema,
            new GenericData
          ).read(null, deserializer)

        Codec[To].decode(read, writerSchema)
    }
}
