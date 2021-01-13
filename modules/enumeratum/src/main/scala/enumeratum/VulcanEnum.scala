/*
 * Copyright 2019-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package enumeratum

import scala.reflect.runtime.universe.WeakTypeTag
import vulcan.Codec

trait VulcanEnum[A <: EnumEntry] { this: Enum[A] =>
  implicit def vulcanCodec(implicit tag: WeakTypeTag[A]): Codec[A] =
    Vulcan.enumCodec(this)
}
