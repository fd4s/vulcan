package vulcan

import cats.data._
import cats.Eq
import cats.implicits._
import java.time.{Instant, LocalDate}
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.Assertion
import scala.collection.immutable.SortedSet
import vulcan.examples._
import java.util.UUID
final class RoundtripSpec extends BaseSpec with RoundtripHelpers {
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

  describe("Byte") { it("roundtrip") { roundtrip[Byte] } }

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

  describe("Char") { it("roundtrip") { roundtrip[Char] } }

  describe("Double") { it("roundtrip") { roundtrip[Double] } }

  describe("Enum") { it("roundtrip") { roundtrip[SealedTraitEnum] } }

  describe("Fixed") { it("roundtrip") { roundtrip[FixedBoolean] } }

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

  describe("Map") { it("roundtrip") { roundtrip[Map[String, Int]] } }

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

  describe("Record") { it("roundtrip") { roundtrip[CaseClassField] } }

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

  describe("Set") { it("roundtrip") { roundtrip[Set[Int]] } }

  describe("Short") { it("roundtrip") { roundtrip[Short] } }

  describe("String") { it("roundtrip") { roundtrip[String] } }

  describe("UUID") {
    it("roundtrip") {
      implicit val uuidArbitrary: Arbitrary[UUID] =
        Arbitrary(arbitrary[Array[Byte]].map(UUID.nameUUIDFromBytes(_)))

      roundtrip[UUID]
    }
  }

  describe("Union") { it("roundtrip") { roundtrip[SealedTraitCaseClass] } }

  describe("Unit") { it("roundtrip") { roundtrip[Unit] } }

  describe("Vector") { it("roundtrip") { roundtrip[Vector[Int]] } }
}

trait RoundtripHelpers {
  self: BaseSpec =>

  def roundtrip[A](
    implicit codec: Codec[A],
    arbitrary: Arbitrary[A],
    eq: Eq[A]
  ): Assertion = {
    forAll { (a: A) =>
      roundtrip(a)
      binaryRoundtrip(a)
      jsonRoundtrip(a)
    }
  }

  def roundtrip[A](a: A)(
    implicit codec: Codec[A],
    eq: Eq[A]
  ): Assertion = {
    val avroSchema = codec.schema
    assert(avroSchema.isRight)

    val encoded = codec.encode(a)
    assert(encoded.isRight)

    val decoded: Either[AvroError, A] = codec.decode(encoded.value, avroSchema.value)
    withClue(s"Actual: $decoded, Expected: ${Right(a)}") {
      assert(decoded === Right(a))
    }
  }

  def binaryRoundtrip[A](a: A)(
    implicit codec: Codec[A],
    eq: Eq[A]
  ): Assertion = {
    val binary = Codec.toBinary(a)
    assert(binary.isRight)

    val decoded = codec.schema.flatMap(Codec.fromBinary[A](binary.value, _))
    withClue(s"Actual: $decoded, Expected: ${Right(a)}") {
      assert(decoded === Right(a))
    }
  }

  def jsonRoundtrip[A](a: A)(
    implicit codec: Codec[A],
    eq: Eq[A]
  ): Assertion = {
    val json = Codec.toJson(a)
    assert(json.isRight)

    val decoded = codec.schema.flatMap(Codec.fromJson[A](json.value, _))
    withClue(s"Actual: $decoded, Expected: ${Right(a)}") {
      assert(decoded === Right(a))
    }
  }
}
