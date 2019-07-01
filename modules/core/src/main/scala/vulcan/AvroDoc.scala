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

package vulcan

import scala.annotation.StaticAnnotation

/**
  * Annotation which can be used to include documentation
  * in derived schemas.
  *
  * The annotation can be used in the following situations.<br>
  * - Annotate a type for enum documentation when using
  *   [[Codec#deriveEnum]].<br>
  * - Annotate a `case class` for record documentation
  *   when using [[Codec#derive]].<br>
  * - Annotate a `case class` parameter for record field
  *   documentation when using [[Codec#derive]].
  */
final class AvroDoc(final val doc: String) extends StaticAnnotation {
  override final def toString: String =
    s"AvroDoc($doc)"
}

private[vulcan] final object AvroDoc {
  final def unapply(avroDoc: AvroDoc): Some[String] =
    Some(avroDoc.doc)
}
