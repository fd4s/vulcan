/*
 * Copyright 2019-2022 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package enumeratum.values

import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

import scala.reflect.runtime.universe.WeakTypeTag
import vulcan.Codec

sealed trait VulcanValueEnum[ValueType, EntryType <: ValueEnumEntry[ValueType]] {
  this: ValueEnum[ValueType, EntryType] =>

  implicit def vulcanCodec(implicit tag: WeakTypeTag[EntryType]): Codec[EntryType]
}

trait ByteVulcanEnum[EntryType <: ByteEnumEntry] extends VulcanValueEnum[Byte, EntryType] {
  this: ValueEnum[Byte, EntryType] =>

  implicit override def vulcanCodec(
    implicit tag: WeakTypeTag[EntryType]
  ): Codec.Aux[Int, EntryType] =
    Vulcan.codec(this)
}

trait CharVulcanEnum[EntryType <: CharEnumEntry] extends VulcanValueEnum[Char, EntryType] {
  this: ValueEnum[Char, EntryType] =>

  implicit override def vulcanCodec(
    implicit tag: WeakTypeTag[EntryType]
  ): Codec.Aux[Utf8, EntryType] =
    Vulcan.codec(this)
}

trait IntVulcanEnum[EntryType <: IntEnumEntry] extends VulcanValueEnum[Int, EntryType] {
  this: ValueEnum[Int, EntryType] =>

  implicit override def vulcanCodec(
    implicit tag: WeakTypeTag[EntryType]
  ): Codec.Aux[Int, EntryType] =
    Vulcan.codec(this)
}

trait LongVulcanEnum[EntryType <: LongEnumEntry] extends VulcanValueEnum[Long, EntryType] {
  this: ValueEnum[Long, EntryType] =>

  implicit override def vulcanCodec(
    implicit tag: WeakTypeTag[EntryType]
  ): Codec.Aux[Long, EntryType] =
    Vulcan.codec(this)
}

trait ShortVulcanEnum[EntryType <: ShortEnumEntry] extends VulcanValueEnum[Short, EntryType] {
  this: ValueEnum[Short, EntryType] =>

  implicit override def vulcanCodec(
    implicit tag: WeakTypeTag[EntryType]
  ): Codec.Aux[Int, EntryType] =
    Vulcan.codec(this)
}

trait StringVulcanEnum[EntryType <: StringEnumEntry] extends VulcanValueEnum[String, EntryType] {
  this: ValueEnum[String, EntryType] =>

  implicit override def vulcanCodec(
    implicit tag: WeakTypeTag[EntryType]
  ): Codec.Aux[GenericData.EnumSymbol, EntryType] =
    Vulcan.enumCodec(this)
}
