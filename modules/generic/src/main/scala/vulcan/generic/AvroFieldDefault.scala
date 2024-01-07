/*
 * Copyright 2019-2023 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import scala.annotation.StaticAnnotation

/**
  * Annotation which can be used to set a default value for an Avro field.
  *
  * The annotation can be used in the following situations.<br>
  * - Annotate a field in a case class using `Codec.derive`
  *
  * @see https://avro.apache.org/docs/1.8.1/spec.html#Unions
  *
  *  (Note that when a default value is specified for a record field whose type is a union,
  *  the type of the default value must match the first element of the union.
  *  Thus, for unions containing "null", the "null" is usually listed first, since the default value of such unions is typically null.)
  */
final class AvroFieldDefault[A](final val value: A) extends StaticAnnotation {
  override final def toString: String =
    s"AvroFieldDefault($value)"
}

private[vulcan] object AvroFieldDefault {
  final def unapply[A](avroFieldDefault: AvroFieldDefault[A]): Some[A] =
    Some(avroFieldDefault.value)
}
