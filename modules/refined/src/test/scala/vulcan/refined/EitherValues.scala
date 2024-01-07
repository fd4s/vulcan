/*
 * Copyright 2019-2024 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.refined

trait EitherValues {
  implicit final class EitherValuesSyntax[A, B](val e: Either[A, B]) {
    def value: B = e.getOrElse(throw new NoSuchElementException(s"Expected Right was $e"))
  }
}
