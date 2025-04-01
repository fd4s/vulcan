/*
 * Copyright 2019-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

/**
  * `Throwable` representation of an [[AvroError]],
  * created with [[AvroError#throwable]].
  */
sealed abstract class AvroException(message: String, cause: Option[Throwable])
    extends RuntimeException(message, cause.orNull)

private[vulcan] object AvroException {
  final def apply(message: String, cause: Option[Throwable] = None): AvroException =
    new AvroException(message, cause) {
      override final def toString: String =
        s"vulcan.AvroException: $getMessage"
    }
}
