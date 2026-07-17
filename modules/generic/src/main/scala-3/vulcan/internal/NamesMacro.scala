/*
 * Copyright 2019 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan.internal

import scala.quoted.*
import scala.compiletime.*
import vulcan.internal.Names
import vulcan.generic.AvroDoc
import vulcan.generic.AvroName
import vulcan.generic.AvroNamespace
import vulcan.generic.AvroAlias

object NamesMacro {
  inline def names[A]: Names[A] = ${ namesImpl[A] }

  def namesImpl[A: Type](using Quotes): Expr[Names[A]] =
    import quotes.reflect.*
    val symbol = TypeTree.of[A].symbol

    def getAnnotValue[Ann: Type]: Option[Expr[Ann]] =
      symbol.getAnnotation(TypeRepr.of[Ann].typeSymbol).map(p => p.asExprOf[Ann])

    def toExprOpt(e: Option[Expr[String]]): Expr[Option[String]] =
      e match {
        case Some(stringExp) => '{ Some($stringExp) }
        case None            => '{ Option.empty[String] }
      }

    val typeName = getAnnotValue[AvroName].map(e => '{ $e.name }).getOrElse(Expr(symbol.name))
    val namespace = getAnnotValue[AvroNamespace]
      .map(e => '{ $e.namespace })
      .getOrElse(Expr(symbol.owner.fullName))

    val doc = toExprOpt(getAnnotValue[AvroDoc].map(e => '{ $e.doc }))
    val alias = toExprOpt(getAnnotValue[AvroAlias].map(e => '{ $e.alias }))

    '{ Names[A]($typeName, $namespace, $doc, $alias) }

}
