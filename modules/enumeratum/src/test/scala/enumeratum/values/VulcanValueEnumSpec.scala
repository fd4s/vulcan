package enumeratum.values

import enumeratum.EitherValues
import org.scalacheck.Gen
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import vulcan.Codec
import vulcan.generic.{AvroDoc, AvroNamespace}

final class VulcanValueEnumSpec extends AnyFunSpec with ScalaCheckPropertyChecks with EitherValues {
  describe("ByteVulcanEnum") {
    sealed abstract class CustomEnum(val value: Byte) extends ByteEnumEntry

    object CustomEnum extends ByteEnum[CustomEnum] with ByteVulcanEnum[CustomEnum] {
      case object First extends CustomEnum(1)
      case object Second extends CustomEnum(2)
      case object Third extends CustomEnum(3)

      val values = findValues

      override def withValueOpt(i: Byte): Option[CustomEnum] =
        if (i == 3) None
        else super.withValueOpt(i)
    }

    it("schema should be same as for underlying type") {
      assert {
        Codec[CustomEnum].schema.value.toString ===
          Codec[Byte].schema.value.toString
      }
    }

    it("should roundtrip enumeration values") {
      val gen = Gen.oneOf[CustomEnum](CustomEnum.First, CustomEnum.Second)
      forAll(gen) { customEnum =>
        val roundtrip = Codec.encode(customEnum).flatMap(Codec.decode[CustomEnum])
        assert(roundtrip.value === customEnum)
      }
    }

    it("should error if withValueOpt does not handle schema value") {
      val roundtrip = Codec.encode[CustomEnum](CustomEnum.Third).flatMap(Codec.decode[CustomEnum])
      assert {
        roundtrip.swap.value.message ===
          """3 is not a member of CustomEnum (1, 2, 3)"""
      }
    }
  }

  describe("CharVulcanEnum") {
    sealed abstract class CustomEnum(val value: Char) extends CharEnumEntry

    object CustomEnum extends CharEnum[CustomEnum] with CharVulcanEnum[CustomEnum] {
      case object First extends CustomEnum('1')
      case object Second extends CustomEnum('2')
      case object Third extends CustomEnum('3')

      val values = findValues

      override def withValueOpt(c: Char): Option[CustomEnum] =
        if (c == '3') None
        else super.withValueOpt(c)
    }

    it("schema should be same as for underlying type") {
      assert {
        Codec[CustomEnum].schema.value.toString ===
          Codec[Char].schema.value.toString
      }
    }

    it("should roundtrip enumeration values") {
      val gen = Gen.oneOf[CustomEnum](CustomEnum.First, CustomEnum.Second)
      forAll(gen) { customEnum =>
        val roundtrip = Codec.encode(customEnum).flatMap(Codec.decode[CustomEnum])
        assert(roundtrip.value === customEnum)
      }
    }

    it("should error if withValueOpt does not handle schema value") {
      val roundtrip = Codec.encode[CustomEnum](CustomEnum.Third).flatMap(Codec.decode[CustomEnum])
      assert {
        roundtrip.swap.value.message ===
          """3 is not a member of CustomEnum (1, 2, 3)"""
      }
    }
  }

  describe("IntVulcanEnum") {
    sealed abstract class CustomEnum(val value: Int) extends IntEnumEntry

    object CustomEnum extends IntEnum[CustomEnum] with IntVulcanEnum[CustomEnum] {
      case object First extends CustomEnum(1)
      case object Second extends CustomEnum(2)
      case object Third extends CustomEnum(3)

      val values = findValues

      override def withValueOpt(i: Int): Option[CustomEnum] =
        if (i == 3) None
        else super.withValueOpt(i)
    }

    it("schema should be same as for underlying type") {
      assert {
        Codec[CustomEnum].schema.value.toString ===
          Codec[Int].schema.value.toString
      }
    }

    it("should roundtrip enumeration values") {
      val gen = Gen.oneOf[CustomEnum](CustomEnum.First, CustomEnum.Second)
      forAll(gen) { customEnum =>
        val roundtrip = Codec.encode(customEnum).flatMap(Codec.decode[CustomEnum])
        assert(roundtrip.value === customEnum)
      }
    }

    it("should error if withValueOpt does not handle schema value") {
      val roundtrip = Codec.encode[CustomEnum](CustomEnum.Third).flatMap(Codec.decode[CustomEnum])
      assert {
        roundtrip.swap.value.message ===
          """3 is not a member of CustomEnum (1, 2, 3)"""
      }
    }
  }

  describe("LongVulcanEnum") {
    sealed abstract class CustomEnum(val value: Long) extends LongEnumEntry

    object CustomEnum extends LongEnum[CustomEnum] with LongVulcanEnum[CustomEnum] {
      case object First extends CustomEnum(1L)
      case object Second extends CustomEnum(2L)
      case object Third extends CustomEnum(3L)

      val values = findValues

      override def withValueOpt(l: Long): Option[CustomEnum] =
        if (l == 3L) None
        else super.withValueOpt(l)
    }

    it("schema should be same as for underlying type") {
      assert {
        Codec[CustomEnum].schema.value.toString ===
          Codec[Long].schema.value.toString
      }
    }

    it("should roundtrip enumeration values") {
      val gen = Gen.oneOf[CustomEnum](CustomEnum.First, CustomEnum.Second)
      forAll(gen) { customEnum =>
        val roundtrip = Codec.encode(customEnum).flatMap(Codec.decode[CustomEnum])
        assert(roundtrip.value === customEnum)
      }
    }

    it("should error if withValueOpt does not handle schema value") {
      val roundtrip = Codec.encode[CustomEnum](CustomEnum.Third).flatMap(Codec.decode[CustomEnum])
      assert {
        roundtrip.swap.value.message ===
          """3 is not a member of CustomEnum (1, 2, 3)"""
      }
    }
  }

  describe("ShortVulcanEnum") {
    sealed abstract class CustomEnum(val value: Short) extends ShortEnumEntry

    object CustomEnum extends ShortEnum[CustomEnum] with ShortVulcanEnum[CustomEnum] {
      case object First extends CustomEnum(1)
      case object Second extends CustomEnum(2)
      case object Third extends CustomEnum(3)

      val values = findValues

      override def withValueOpt(s: Short): Option[CustomEnum] =
        if (s == 3) None
        else super.withValueOpt(s)
    }

    it("schema should be same as for underlying type") {
      assert {
        Codec[CustomEnum].schema.value.toString ===
          Codec[Short].schema.value.toString
      }
    }

    it("should roundtrip enumeration values") {
      val gen = Gen.oneOf[CustomEnum](CustomEnum.First, CustomEnum.Second)
      forAll(gen) { customEnum =>
        val roundtrip = Codec.encode(customEnum).flatMap(Codec.decode[CustomEnum])
        assert(roundtrip.value === customEnum)
      }
    }

    it("should error if withValueOpt does not handle schema value") {
      val roundtrip = Codec.encode[CustomEnum](CustomEnum.Third).flatMap(Codec.decode[CustomEnum])
      assert {
        roundtrip.swap.value.message ===
          """3 is not a member of CustomEnum (1, 2, 3)"""
      }
    }
  }

  describe("StringVulcanEnum") {

    @AvroNamespace("com.example")
    @AvroDoc("Custom enumeration")
    sealed abstract class CustomEnum(val value: String) extends StringEnumEntry

    object CustomEnum extends StringEnum[CustomEnum] with StringVulcanEnum[CustomEnum] {
      case object First extends CustomEnum("first")
      case object Second extends CustomEnum("second")
      case object Third extends CustomEnum("third")

      val values = findValues

      override def withValueOpt(s: String): Option[CustomEnum] =
        if (s == "third") None
        else super.withValueOpt(s)
    }

    it("schema should be enum") {
      assert {
        Codec[CustomEnum].schema.value.toString ===
          """{"type":"enum","name":"CustomEnum","namespace":"com.example","doc":"Custom enumeration","symbols":["first","second","third"]}"""
      }
    }

    it("should roundtrip enumeration values") {
      val gen = Gen.oneOf[CustomEnum](CustomEnum.First, CustomEnum.Second)
      forAll(gen) { customEnum =>
        val roundtrip = Codec.encode(customEnum).flatMap(Codec.decode[CustomEnum])
        assert(roundtrip.value === customEnum)
      }
    }

    it("should error if withValueOpt does not handle schema value") {
      val roundtrip = Codec.encode[CustomEnum](CustomEnum.Third).flatMap(Codec.decode[CustomEnum])
      assert {
        roundtrip.swap.value.message ===
          """Error decoding com.example.CustomEnum: third is not a member of CustomEnum (first, second, third)"""
      }
    }
  }
}
