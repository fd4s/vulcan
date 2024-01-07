/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import cats.Eq
import cats.implicits._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalacheck.Arbitrary
import org.scalatest.Assertion
import vulcan._

class RoundtripBase extends BaseSpec {

  def roundtrip[A](
    implicit codec: Codec[A],
    arbitrary: Arbitrary[A],
    eq: Eq[A]
  ): Assertion = {
    forAll { (a: A) =>
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

    val encoded = codec.encode(a)
    assert(encoded.isRight)

    val decoded = codec.decode(encoded.value, avroSchema.value)
    assert(decoded === Right(a))
  }

  def binaryRoundtrip[A](a: A)(
    implicit codec: Codec[A],
    eq: Eq[A]
  ): Assertion = {
    val binary = toBinary(a)
    assert(binary.isRight)

    val decoded = fromBinary[A](binary.value)
    assert(decoded === Right(a))
  }

  def toBinary[A](a: A)(
    implicit codec: Codec[A]
  ): Either[AvroError, Array[Byte]] =
    codec.schema.flatMap { schema =>
      codec.encode(a).map { encoded =>
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
