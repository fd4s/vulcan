/*
 * Copyright 2019-2025 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.internal

import cats.data.Chain

private[vulcan] object syntax {
  implicit class ChainOps[A](val self: Chain[A]) extends AnyVal {
    def foreach(f: A => Unit): Unit = self.foldLeft(())((_, a) => f(a))
  }
}
