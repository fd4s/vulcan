/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import scala.annotation.StaticAnnotation

/**
  * Annotation which can be used to enable/disable explicit default null values for nullable fields
  * in derived schemas.
  *
  * The annotation can be used in the following situations.<br>
  * - Annotate a `case class` to enable/disable explicit default null values in the schema
  *   for all nullable fields when using `Codec.derive` from the generic module.<br>
  * - Annotate a `case class` parameter to enable/disable explicit default null value in the schema
  *   for this specific nullable field when using `Codec.derive` from the
  *   generic module.
  *
  * `Parameter` annotation takes precedence over `case class` one when both are used.
  */
final class AvroNullDefault(final val enabled: Boolean) extends StaticAnnotation {
  override final def toString: String =
    s"AvroNullDefault($enabled)"
}

object AvroNullDefault {
  final def unapply(avroDefaultNulls: AvroNullDefault): Some[Boolean] =
    Some(avroDefaultNulls.enabled)
}
