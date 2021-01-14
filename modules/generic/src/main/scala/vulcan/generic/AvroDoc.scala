/*
 * Copyright 2019-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import scala.annotation.StaticAnnotation

/**
  * Annotation which can be used to include documentation
  * in derived schemas.
  *
  * The annotation can be used in the following situations.<br>
  * - Annotate a type for enum documentation when using
  *   [[Codec.deriveEnum]].<br>
  * - Annotate a type for fixed documentation when using
  *   [[Codec.deriveFixed]].<br>
  * - Annotate a `case class` for record documentation
  *   when using `Codec.derive` from the generic module.<br>
  * - Annotate a `case class` parameter for record field
  *   documentation when using `Codec.derive` from the
  *   generic module.
  */
final class AvroDoc(final val doc: String) extends StaticAnnotation {
  override final def toString: String =
    s"AvroDoc($doc)"
}

private[vulcan] object AvroDoc {
  final def unapply(avroDoc: AvroDoc): Some[String] =
    Some(avroDoc.doc)
}
