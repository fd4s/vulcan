/*
 * Copyright 2019 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package enumeratum.values

import enumeratum.EitherValues
import org.scalacheck.Gen
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import vulcan.Codec

final class VulcanValueEnumSpec extends AnyFunSpec with ScalaCheckPropertyChecks with EitherValues {

  describe("ByteVulcanEnum") {

    it("schema should be same as for underlying type") {
      assert {
        Codec[CustomByteEnum].schema.value.toString ===
          Codec[Byte].schema.value.toString
      }
    }

    it("should roundtrip enumeration values") {
      val gen = Gen.oneOf[CustomByteEnum](CustomByteEnum.First, CustomByteEnum.Second)
      forAll(gen) { customEnum =>
        val roundtrip = Codec.encode(customEnum).flatMap(Codec.decode[CustomByteEnum])
        assert(roundtrip.value === customEnum)
      }
    }

    it("should error if withValueOpt does not handle schema value") {
      val roundtrip =
        Codec.encode[CustomByteEnum](CustomByteEnum.Third).flatMap(Codec.decode[CustomByteEnum])
      assert {
        roundtrip.swap.value.message ===
          """3 is not a member of CustomByteEnum (1, 2, 3)"""
      }
    }
  }

  describe("CharVulcanEnum") {

    it("schema should be same as for underlying type") {
      assert {
        Codec[CustomCharEnum].schema.value.toString ===
          Codec[Char].schema.value.toString
      }
    }

    it("should roundtrip enumeration values") {
      val gen = Gen.oneOf[CustomCharEnum](CustomCharEnum.First, CustomCharEnum.Second)
      forAll(gen) { customEnum =>
        val roundtrip = Codec.encode(customEnum).flatMap(Codec.decode[CustomCharEnum])
        assert(roundtrip.value === customEnum)
      }
    }

    it("should error if withValueOpt does not handle schema value") {
      val roundtrip =
        Codec.encode[CustomCharEnum](CustomCharEnum.Third).flatMap(Codec.decode[CustomCharEnum])
      assert {
        roundtrip.swap.value.message ===
          """3 is not a member of CustomCharEnum (1, 2, 3)"""
      }
    }
  }

  describe("IntVulcanEnum") {

    it("schema should be same as for underlying type") {
      assert {
        Codec[CustomIntEnum].schema.value.toString ===
          Codec[Int].schema.value.toString
      }
    }

    it("should roundtrip enumeration values") {
      val gen = Gen.oneOf[CustomIntEnum](CustomIntEnum.First, CustomIntEnum.Second)
      forAll(gen) { customEnum =>
        val roundtrip = Codec.encode(customEnum).flatMap(Codec.decode[CustomIntEnum])
        assert(roundtrip.value === customEnum)
      }
    }

    it("should error if withValueOpt does not handle schema value") {
      val roundtrip =
        Codec.encode[CustomIntEnum](CustomIntEnum.Third).flatMap(Codec.decode[CustomIntEnum])
      assert {
        roundtrip.swap.value.message ===
          """3 is not a member of CustomIntEnum (1, 2, 3)"""
      }
    }
  }

  describe("LongVulcanEnum") {

    it("schema should be same as for underlying type") {
      assert {
        Codec[CustomLongEnum].schema.value.toString ===
          Codec[Long].schema.value.toString
      }
    }

    it("should roundtrip enumeration values") {
      val gen = Gen.oneOf[CustomLongEnum](CustomLongEnum.First, CustomLongEnum.Second)
      forAll(gen) { customEnum =>
        val roundtrip = Codec.encode(customEnum).flatMap(Codec.decode[CustomLongEnum])
        assert(roundtrip.value === customEnum)
      }
    }

    it("should error if withValueOpt does not handle schema value") {
      val roundtrip =
        Codec.encode[CustomLongEnum](CustomLongEnum.Third).flatMap(Codec.decode[CustomLongEnum])
      assert {
        roundtrip.swap.value.message ===
          """3 is not a member of CustomLongEnum (1, 2, 3)"""
      }
    }
  }

  describe("ShortVulcanEnum") {

    it("schema should be same as for underlying type") {
      assert {
        Codec[CustomShortEnum].schema.value.toString ===
          Codec[Short].schema.value.toString
      }
    }

    it("should roundtrip enumeration values") {
      val gen = Gen.oneOf[CustomShortEnum](CustomShortEnum.First, CustomShortEnum.Second)
      forAll(gen) { customEnum =>
        val roundtrip = Codec.encode(customEnum).flatMap(Codec.decode[CustomShortEnum])
        assert(roundtrip.value === customEnum)
      }
    }

    it("should error if withValueOpt does not handle schema value") {
      val roundtrip =
        Codec.encode[CustomShortEnum](CustomShortEnum.Third).flatMap(Codec.decode[CustomShortEnum])
      assert {
        roundtrip.swap.value.message ===
          """3 is not a member of CustomShortEnum (1, 2, 3)"""
      }
    }
  }

  describe("StringVulcanEnum") {

    it("schema should be enum") {
      assert {
        Codec[CustomStringEnum].schema.value.toString ===
          """{"type":"enum","name":"CustomStringEnum","namespace":"com.example","doc":"Custom enumeration","symbols":["first","second","third"]}"""
      }
    }

    it("should roundtrip enumeration values") {
      val gen = Gen.oneOf[CustomStringEnum](CustomStringEnum.First, CustomStringEnum.Second)
      forAll(gen) { customEnum =>
        val roundtrip = Codec.encode(customEnum).flatMap(Codec.decode[CustomStringEnum])
        assert(roundtrip.value === customEnum)
      }
    }

    it("should error if withValueOpt does not handle schema value") {
      val roundtrip = Codec
        .encode[CustomStringEnum](CustomStringEnum.Third)
        .flatMap(Codec.decode[CustomStringEnum])
      assert {
        roundtrip.swap.value.message ===
          """Error decoding com.example.CustomStringEnum: third is not a member of CustomStringEnum (first, second, third)"""
      }
    }
  }
}
