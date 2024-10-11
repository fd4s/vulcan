/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.generic

import vulcan.Props
import scala.annotation.StaticAnnotation

/**
  * Annotation which can be used to alter props
  * in derived schemas.
  *
  * The annotation can be used in the following situations.<br>
  * - Annotate a type for enum props when using
  *   [[deriveEnum]].<br>
  * - Annotate a type for fixed props when using
  *   [[deriveFixed]].<br>
  * - Annotate a `case class` for record props
  *   when using `Codec.derive` from the generic module.<br>
  * - Annotate a `case class` parameter for record field
  *   props when using `Codec.derive` from the
  *   generic module.
  */
final class AvroProps(final val prop: Props) extends StaticAnnotation {
  override final def toString: String =
    s"AvroProps($prop)"
}

private[vulcan] object AvroProps {
  final def unapply(avroProps: AvroProps): Some[Props] =
    Some(avroProps.prop)
}
