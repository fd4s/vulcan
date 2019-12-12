/*
 * Copyright 2019 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.internal

import java.nio.ByteBuffer
import org.apache.avro.generic.{GenericEnumSymbol, GenericFixed, IndexedRecord}
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
      case record: IndexedRecord =>
        record.getSchema.getFields.asScala.zipWithIndex
          .foldLeft(Map.empty[String, Any]) {
            case (map, (field, index)) =>
              map.updated(field.name, adaptForSchema(record.get(index)))
          }
          .asJava
      case _ =>
        encoded
    }
}
