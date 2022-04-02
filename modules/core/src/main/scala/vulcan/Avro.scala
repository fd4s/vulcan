/*
 * Copyright 2019-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import org.apache.avro.Schema

/*
 * Types modelling the Avro format. Because we use the Apache Avro Java library for serialization to
 * bytes or JSON, these are the same types as those used by that library.
 */
object Avro {
  type Boolean = scala.Boolean
  type Int = scala.Int
  type Long = scala.Long
  type Float = scala.Float
  type Double = scala.Double

  type String = org.apache.avro.util.Utf8
  private[vulcan] object String {
    def apply(s: Predef.String): Avro.String = new org.apache.avro.util.Utf8(s)
  }

  type Bytes = java.nio.ByteBuffer
  type Null = scala.Null

  type Fixed = org.apache.avro.generic.GenericFixed
  private[vulcan] object Fixed {
    def apply(schema: Schema, bytes: scala.Array[Byte]): Avro.Fixed =
      new org.apache.avro.generic.GenericData.Fixed(schema, bytes)
  }

  type Record = org.apache.avro.generic.GenericRecord

  type EnumSymbol = org.apache.avro.generic.GenericData.EnumSymbol
  private[vulcan] object EnumSymbol {
    def apply(schema: Schema, symbol: Predef.String) =
      new org.apache.avro.generic.GenericData.EnumSymbol(schema, symbol)
  }

  type Array[A] = java.util.List[A]
  type Map[A] = java.util.Map[String, A]
}
