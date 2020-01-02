/*
 * Copyright 2019-2020 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package vulcan

import cats.data.{Chain, NonEmptyChain}
import cats.implicits._
import cats.Show
import vulcan.internal.schema.adaptForSchema

/**
  * Custom properties which can be included in a schema.
  *
  * Use [[Props.one]] to create an instance, and
  * [[Props#add]] to add more properties.
  */
sealed abstract class Props {

  /**
    * Returns a new [[Props]] instance including a
    * property with the specified name and value.
    *
    * The value is encoded using the [[Codec]].
    */
  def add[A](name: String, value: A)(implicit codec: Codec[A]): Props

  /**
    * Returns a `Chain` of name-value pairs, where
    * the value has been encoded with a [[Codec]].
    *
    * If encoding of any value resulted in error,
    * instead returns the first such error.
    */
  def toChain: Either[AvroError, Chain[(String, Any)]]
}

final object Props {
  private[this] final class NonEmptyProps(
    props: NonEmptyChain[(String, Either[AvroError, Any])]
  ) extends Props {
    override final def add[A](name: String, value: A)(implicit codec: Codec[A]): Props =
      new NonEmptyProps(props.append(name -> encodeForSchema(value)))

    override final def toChain: Either[AvroError, Chain[(String, Any)]] =
      props.toChain.traverse {
        case (name, value) =>
          value.tupleLeft(name)
      }

    override final def toString: String =
      toChain match {
        case Right(props) =>
          props.toList
            .map { case (name, value) => s"$name -> $value" }
            .mkString("Props(", ", ", ")")

        case Left(error) =>
          error.show
      }
  }

  private[this] final object EmptyProps extends Props {
    override final def add[A](name: String, value: A)(implicit codec: Codec[A]): Props =
      Props.one(name, value)

    override final val toChain: Either[AvroError, Chain[(String, Any)]] =
      Right(Chain.empty)

    override final def toString: String =
      "Props()"
  }

  private[this] final def encodeForSchema[A](a: A)(
    implicit codec: Codec[A]
  ): Either[AvroError, Any] =
    Codec.encode(a).map(adaptForSchema)

  /**
    * Returns a new [[Props]] instance including a
    * property with the specified name and value.
    *
    * The value is encoded using the [[Codec]].
    */
  final def one[A](name: String, value: A)(implicit codec: Codec[A]): Props =
    new NonEmptyProps(NonEmptyChain.one(name -> encodeForSchema(value)))

  /**
    * The [[Props]] instance without any properties.
    */
  final val empty: Props =
    EmptyProps

  implicit final val propsShow: Show[Props] =
    Show.fromToString
}
