package vulcan

import cats.data._
import cats.Eq
import cats.implicits._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.time.{Instant, LocalDate}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.Assertion
import scala.collection.immutable.SortedSet
import vulcan.examples._
import java.util.UUID
import shapeless.{:+:, CNil, Coproduct}

final class RoundtripSpec extends BaseSpec {
  describe("BigDecimal") {
    it("roundtrip") {
      implicit val bigDecimalCodec: Codec[BigDecimal] =
        Codec.decimal(precision = 10, scale = 5)

      implicit val bigDecimalArbitrary: Arbitrary[BigDecimal] =
        Arbitrary {
          Arbitrary.arbBigDecimal.arbitrary
            .map(_.setScale(5, scala.math.BigDecimal.RoundingMode.HALF_UP))
            .map { decimal =>
              if (decimal.precision > 10)
                (decimal * BigDecimal(10).pow(10 - decimal.precision))
                  .setScale(5, scala.math.BigDecimal.RoundingMode.HALF_UP)
              else decimal
            }
        }

      roundtrip[BigDecimal]
    }
  }

  describe("Boolean") { it("roundtrip") { roundtrip[Boolean] } }

  describe("Array[Byte]") {
    it("roundtrip") {
      implicit val arrayByteEq: Eq[Array[Byte]] =
        Eq.instance { (a, b) =>
          a.size == b.size && a.zip(b).forall { case (b1, b2) => b1 == b2 }
        }

      roundtrip[Array[Byte]]
    }
  }

  describe("Chain") {
    it("roundtrip") {
      implicit def arbitraryChain[A](
        implicit arbitrary: Arbitrary[A]
      ): Arbitrary[Chain[A]] =
        Arbitrary(Gen.listOf(arbitrary.arbitrary).map(Chain.fromSeq))

      roundtrip[Chain[Int]]
    }
  }

  describe("Coproduct") {
    it("roundtrip") {
      type Types = CaseClassField :+: Int :+: CaseClassAvroDoc :+: CNil

      implicit val arbitraryTypes: Arbitrary[Types] =
        Arbitrary {
          Gen.oneOf(
            arbitrary[Int].map(n => Coproduct[Types](CaseClassField(n))),
            arbitrary[Int].map(n => Coproduct[Types](n)),
            arbitrary[Option[String]].map(os => Coproduct[Types](CaseClassAvroDoc(os)))
          )
        }

      implicit val eqTypes: Eq[Types] =
        Eq.fromUniversalEquals

      roundtrip[Types]
    }
  }

  describe("derived.caseClass") {
    it("CaseClassField") { roundtrip[CaseClassField] }
    it("CaseClassAvroNamespace") { roundtrip[CaseClassAvroNamespace] }
  }

  describe("derived.sealedTrait") {
    it("SealedTraitCaseClassAvroNamespace") { roundtrip[SealedTraitCaseClassAvroNamespace] }
    it("SealedTraitCaseClassCustom") { roundtrip[SealedTraitCaseClassCustom] }
  }

  describe("derived.enum") {
    it("SealedTraitEnumDerived") { roundtrip[SealedTraitEnumDerived] }
  }

  describe("Double") { it("roundtrip") { roundtrip[Double] } }

  describe("Enum") { it("roundtrip") { roundtrip[SealedTraitEnum] } }

  describe("Float") { it("roundtrip") { roundtrip[Float] } }

  describe("Instant") {
    it("roundtrip") {
      implicit val instantArbitrary: Arbitrary[Instant] =
        Arbitrary(Gen.posNum[Long].map { millis =>
          val instant = Instant.ofEpochMilli(millis)
          instant.minusNanos(instant.getNano().toLong)
        })

      implicit val instantEq: Eq[Instant] =
        Eq.instance(_ equals _)

      roundtrip[Instant]
    }
  }

  describe("Int") { it("roundtrip") { roundtrip[Int] } }

  describe("List") { it("roundtrip") { roundtrip[List[Int]] } }

  describe("LocalDate") {
    it("roundtrip") {
      implicit val localDateArbitrary: Arbitrary[LocalDate] =
        Arbitrary(Gen.posNum[Int].map(day => LocalDate.ofEpochDay(day.toLong)))

      implicit val localDateEq: Eq[LocalDate] =
        Eq.instance(_ isEqual _)

      roundtrip[LocalDate]
    }
  }

  describe("Long") { it("roundtrip") { roundtrip[Long] } }

  describe("NonEmptyChain") {
    it("roundtrip") {
      implicit def arbitraryNonEmptyChain[A](
        implicit arbitrary: Arbitrary[A]
      ): Arbitrary[NonEmptyChain[A]] =
        Arbitrary(
          Gen
            .nonEmptyListOf(arbitrary.arbitrary)
            .map(nel => NonEmptyChain.fromChainUnsafe(Chain.fromSeq(nel)))
        )

      roundtrip[NonEmptyChain[Int]]
    }
  }

  describe("NonEmptyList") {
    it("roundtrip") {
      implicit def arbitraryNonEmptyList[A](
        implicit arbitrary: Arbitrary[A]
      ): Arbitrary[NonEmptyList[A]] =
        Arbitrary(
          Gen
            .nonEmptyListOf(arbitrary.arbitrary)
            .map(NonEmptyList.fromListUnsafe)
        )

      roundtrip[NonEmptyList[Int]]
    }
  }

  describe("NonEmptySet") {
    it("roundtrip") {
      implicit def arbitraryNonEmptySet[A](
        implicit arbitrary: Arbitrary[A],
        ordering: Ordering[A]
      ): Arbitrary[NonEmptySet[A]] =
        Arbitrary(
          Gen
            .nonEmptyListOf(arbitrary.arbitrary)
            .map(nel => NonEmptySet.fromSetUnsafe(SortedSet(nel: _*)))
        )

      roundtrip[NonEmptySet[Int]]
    }
  }

  describe("NonEmptyVector") {
    it("roundtrip") {
      implicit def arbitraryNonEmptyVector[A](
        implicit arbitrary: Arbitrary[A]
      ): Arbitrary[NonEmptyVector[A]] =
        Arbitrary(
          Gen
            .nonEmptyListOf(arbitrary.arbitrary)
            .map(nel => NonEmptyVector.fromVectorUnsafe(nel.toVector))
        )

      roundtrip[NonEmptyVector[Int]]
    }
  }

  describe("Option") { it("roundtrip") { roundtrip[Option[Int]] } }

  describe("Seq") {
    it("roundtrip") {
      implicit def seqArbitrary[A](
        implicit arbitrary: Arbitrary[A]
      ): Arbitrary[Seq[A]] =
        Arbitrary(Gen.listOf(arbitrary.arbitrary))

      implicit def seqEq[A](
        implicit eq: Eq[A]
      ): Eq[Seq[A]] =
        Eq.instance { (a, b) =>
          a.size == b.size && a.zip(b).forall { case (a1, a2) => eq.eqv(a1, a2) }
        }

      roundtrip[Seq[Int]]
    }
  }

  describe("Record") {
    it("roundtrip") {
      implicit val caseClassFieldCodec: Codec[CaseClassField] =
        Codec.record("CaseClassField") { field =>
          field("value", _.value).map(CaseClassField(_))
        }

      roundtrip[CaseClassField]
    }
  }

  describe("Set") { it("roundtrip") { roundtrip[Set[Int]] } }

  describe("String") { it("roundtrip") { roundtrip[String] } }

  describe("UUID") {
    it("roundtrip") {
      implicit val uuidArbitrary: Arbitrary[UUID] =
        Arbitrary(arbitrary[Array[Byte]].map(UUID.nameUUIDFromBytes(_)))

      roundtrip[UUID]
    }
  }

  describe("Unit") { it("roundtrip") { roundtrip[Unit] } }

  describe("Vector") { it("roundtrip") { roundtrip[Vector[Int]] } }

  def roundtrip[A](
    implicit codec: Codec[A],
    arbitrary: Arbitrary[A],
    eq: Eq[A]
  ): Assertion = {
    forAll { a: A =>
      roundtrip(a)
      binaryRoundtrip(a)
    }
  }

  def roundtrip[A](a: A)(
    implicit codec: Codec[A],
    eq: Eq[A]
  ): Assertion = {
    val avroSchema = codec.schema
    assert(avroSchema.isRight)

    val encoded = codec.encode(a, avroSchema.right.get)
    assert(encoded.isRight)

    val decoded = codec.decode(encoded.right.get, avroSchema.right.get)
    assert(decoded === Right(a))
  }

  def binaryRoundtrip[A](a: A)(
    implicit codec: Codec[A],
    eq: Eq[A]
  ): Assertion = {
    val binary = toBinary(a)
    assert(binary.isRight)

    val decoded = fromBinary(binary.right.get)
    assert(decoded === Right(a))
  }

  def toBinary[A](a: A)(
    implicit codec: Codec[A]
  ): Either[AvroError, Array[Byte]] =
    codec.schema.flatMap { schema =>
      codec.encode(a, schema).map { encoded =>
        val baos = new ByteArrayOutputStream()
        val serializer = EncoderFactory.get().binaryEncoder(baos, null)
        new GenericDatumWriter[Any](schema)
          .write(encoded, serializer)
        serializer.flush()
        baos.toByteArray()
      }
    }

  def fromBinary[A](bytes: Array[Byte])(
    implicit codec: Codec[A]
  ): Either[AvroError, A] =
    codec.schema.flatMap { schema =>
      val bais = new ByteArrayInputStream(bytes)
      val deserializer = DecoderFactory.get().binaryDecoder(bais, null)
      val read =
        new GenericDatumReader[Any](
          schema,
          schema,
          new GenericData
        ).read(null, deserializer)

      codec.decode(read, schema)
    }
}
