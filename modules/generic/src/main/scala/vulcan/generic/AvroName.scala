/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import scala.annotation.StaticAnnotation

/**
  * Annotation which can be used to alter the record name
  * in derived schemas.
  *
  * The annotation can be used in the following situations.<br>
  * - Annotate a type for enum name when using
  *   [[deriveEnum]].<br>
  * - Annotate a type for fixed name when using
  *   [[deriveFixed]].<br>
  * - Annotate a `case class` for record name
  *   when using `Codec.derive` from the generic module.<br>
  * - Annotate a `case class` parameter for record field
  *   name when using `Codec.derive` from the
  *   generic module.
  */
final class AvroName(final val name: String) extends StaticAnnotation {
  override final def toString: String =
    s"AvroName($name)"
}

private[vulcan] object AvroName {
  final def unapply(avroName: AvroName): Some[String] =
    Some(avroName.name)
}
