/*
 * Copyright 2019-2021 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import io.chrisdavenport.fuuid.FUUID

import java.util.UUID

package object fuuid {
  implicit val fuuidCodec: Codec[FUUID] = Codec[UUID].imap(FUUID.fromUUID)(FUUID.Unsafe.toUUID)
}
