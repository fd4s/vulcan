/*
 * Copyright 2019-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.internal

import scala.reflect.runtime.universe.WeakTypeTag

private[vulcan] object tags {
  final def docFrom[A](tag: WeakTypeTag[A]): Option[String] =
    tag.tpe.typeSymbol.annotations.collectFirst {
      case annotation
          if annotation.tree.tpe.typeSymbol.fullName == "vulcan.AvroDoc" ||
            annotation.tree.tpe.typeSymbol.fullName == "vulcan.generic.AvroDoc" =>
        val doc = annotation.tree.children.last.toString
        doc.substring(1, doc.length - 1)
    }

  final def nameFrom[A](tag: WeakTypeTag[A]): String =
    tag.tpe.typeSymbol.annotations
      .collectFirst {
        case annotation
          if annotation.tree.tpe.typeSymbol.fullName == "vulcan.generic.AvroName" =>
          val name = annotation.tree.children.last.toString
          name.substring(1, name.length - 1)
      }
      .getOrElse {
        tag.tpe.typeSymbol.name.decodedName.toString
      }

  final def namespaceFrom[A](tag: WeakTypeTag[A]): String =
    tag.tpe.typeSymbol.annotations
      .collectFirst {
        case annotation
            if annotation.tree.tpe.typeSymbol.fullName == "vulcan.AvroNamespace" ||
              annotation.tree.tpe.typeSymbol.fullName == "vulcan.generic.AvroNamespace" =>
          val namespace = annotation.tree.children.last.toString
          namespace.substring(1, namespace.length - 1)
      }
      .getOrElse {
        tag.tpe.typeSymbol.fullName.dropRight(tag.tpe.typeSymbol.name.decodedName.toString.length + 1)
      }
}
