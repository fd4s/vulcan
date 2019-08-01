/*
 * Copyright 2019 OVO Energy Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vulcan.internal

import scala.reflect.runtime.universe.WeakTypeTag

private[vulcan] final object tags {
  final def docFrom[A](tag: WeakTypeTag[A]): Option[String] =
    tag.tpe.typeSymbol.annotations.collectFirst {
      case annotation if annotation.tree.tpe.typeSymbol.fullName == "vulcan.AvroDoc" =>
        val doc = annotation.tree.children.last.toString
        doc.substring(1, doc.length - 1)
    }

  final def fullNameFrom[A](tag: WeakTypeTag[A]): String =
    tag.tpe.typeSymbol.fullName

  final def nameFrom[A](tag: WeakTypeTag[A]): String =
    tag.tpe.typeSymbol.name.decodedName.toString

  final def namespaceFrom[A](tag: WeakTypeTag[A]): String =
    tag.tpe.typeSymbol.annotations
      .collectFirst {
        case annotation if annotation.tree.tpe.typeSymbol.fullName == "vulcan.AvroNamespace" =>
          val namespace = annotation.tree.children.last.toString
          namespace.substring(1, namespace.length - 1)
      }
      .getOrElse {
        tag.tpe.typeSymbol.fullName.dropRight(nameFrom(tag).length + 1)
      }
}
