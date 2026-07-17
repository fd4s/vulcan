/*
 * Copyright 2019 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.internal
import vulcan.internal.NamesMacro

case class Names[A](
  typeName: String,
  namespace: String,
  doc: Option[String],
  aliasOf: Option[String]
)

object Names {

  inline given [A]: Names[A] = NamesMacro.names[A]
}
