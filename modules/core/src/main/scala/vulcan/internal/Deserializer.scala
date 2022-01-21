/*
 * Copyright 2019-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan
package internal

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.DecoderFactory

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

private[vulcan] object Deserializer {
  def fromBinary[A](bytes: Array[Byte], writerSchema: Schema)(
    implicit codec: Codec[A]
  ): Either[AvroError, A] =
    AvroError.catchNonFatal {
      val bais = new ByteArrayInputStream(bytes)
      val decoder = DecoderFactory.get.binaryDecoder(bais, null)
      val value = new GenericDatumReader[Any](writerSchema).read(null, decoder)
      codec.decode(value, writerSchema)
    }

  def fromJson[A](json: String, writerSchema: Schema)(
    implicit codec: Codec[A]
  ): Either[AvroError, A] =
    AvroError.catchNonFatal {
      val bais = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))
      val decoder = DecoderFactory.get.jsonDecoder(writerSchema, bais)
      val value = new GenericDatumReader[Any](writerSchema).read(null, decoder)
      codec.decode(value, writerSchema)
    }
}
