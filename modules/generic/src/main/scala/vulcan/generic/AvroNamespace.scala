/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import scala.annotation.StaticAnnotation

/**
  * Annotation which can be used to set the namespace
  * in derived schemas.
  *
  * The annotation can be used in the following situations.<br>
  * - Annotate a type for enum namespace when using
  *   [[deriveEnum]].<br>
  * - Annotate a type for fixed namespace when using
  *   [[deriveFixed]].<br>
  * - Annotate a `case class` for record namespace when
  *   using `Codec.derive` from the generic module.
  */
final class AvroNamespace(final val namespace: String) extends StaticAnnotation {
  override final def toString: String =
    s"AvroNamespace($namespace)"
}

private[vulcan] object AvroNamespace {
  final def unapply(avroNamespace: AvroNamespace): Some[String] =
    Some(avroNamespace.namespace)
}
