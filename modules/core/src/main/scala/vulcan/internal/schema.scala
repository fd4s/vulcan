/*
 * Copyright 2019 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.internal

import java.nio.ByteBuffer
import org.apache.avro.generic.{GenericEnumSymbol, GenericFixed, GenericRecord}
import vulcan.internal.converters.collection._

private[vulcan] final object schema {
  final def adaptForSchema(encoded: Any): Any =
    encoded match {
      case bytes: ByteBuffer =>
        bytes.array()
      case enum: GenericEnumSymbol[_] =>
        enum.toString()
      case fixed: GenericFixed =>
        fixed.bytes()
      case record: GenericRecord =>
        record.getSchema.getFields.asScala
          .foldLeft(Map.empty[String, Any]) {
            case (map, field) =>
              map.updated(field.name, adaptForSchema(record.get(field.name)))
          }
          .asJava
      case _ =>
        encoded
    }
}
