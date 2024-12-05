/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import scala.annotation.StaticAnnotation

/**
  * Annotation which can be used to include the record alias
  * in derived schemas.
  *
  * The annotation can be used in the following situations.<br>
  * - Annotate a type for enum alias when using
  *   [[deriveEnum]].<br>
  * - Annotate a type for fixed alias when using
  *   [[deriveFixed]].<br>
  * - Annotate a `case class` for record alias
  *   when using `Codec.derive` from the generic module.<br>
  * - Annotate a `case class` parameter for record field
  *   alias when using `Codec.derive` from the
  *   generic module.
  */
final class AvroAlias(final val alias: String) extends StaticAnnotation {
  override final def toString: String =
    s"AvroAlias($alias)"
}

private[vulcan] object AvroAlias {
  final def unapply(avroAlias: AvroAlias): Some[String] =
    Some(avroAlias.alias)
}
