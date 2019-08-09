/*
 * Copyright 2019 OVO Energy Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vulcan.internal

import java.nio.ByteBuffer
import org.apache.avro.generic.{GenericEnumSymbol, GenericFixed, IndexedRecord}
import vulcan.internal.converters.collection._

private[vulcan] final object schema {
  final def defaultFrom(encoded: Any): Any =
    encoded match {
      case bytes: ByteBuffer =>
        bytes.array()
      case enum: GenericEnumSymbol[_] =>
        enum.toString()
      case fixed: GenericFixed =>
        fixed.bytes()
      case record: IndexedRecord =>
        val fields = record.getSchema().getFields().asScala
        fields
          .foldLeft(Map.empty[String, Any]) { (map, field) =>
            val value = record.get(fields.indexOf(field))
            map.updated(field.name(), defaultFrom(value))
          }
          .asJava
      case other => other
    }
}
