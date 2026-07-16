/*
 * Copyright 2019 OVO Energy Limited
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package enumeratum.values

import vulcan.{Avro, Codec}
import vulcan.internal.Names

sealed trait VulcanValueEnum[ValueType, EntryType <: ValueEnumEntry[ValueType]] {
  this: ValueEnum[ValueType, EntryType] =>

  given vulcanCodec(using names: Names[EntryType]): Codec[EntryType]
}

trait ByteVulcanEnum[EntryType <: ByteEnumEntry] extends VulcanValueEnum[Byte, EntryType] {
  this: ValueEnum[Byte, EntryType] =>

  given vulcanCodec(using names: Names[EntryType]): Codec.Aux[Avro.Int, EntryType] =
    Vulcan.codec(this)
}

trait CharVulcanEnum[EntryType <: CharEnumEntry] extends VulcanValueEnum[Char, EntryType] {
  this: ValueEnum[Char, EntryType] =>

  given vulcanCodec(using names: Names[EntryType]): Codec.Aux[Avro.String, EntryType] =
    Vulcan.codec(this)
}

trait IntVulcanEnum[EntryType <: IntEnumEntry] extends VulcanValueEnum[Int, EntryType] {
  this: ValueEnum[Int, EntryType] =>

  given vulcanCodec(using names: Names[EntryType]): Codec.Aux[Avro.Int, EntryType] =
    Vulcan.codec(this)
}

trait LongVulcanEnum[EntryType <: LongEnumEntry] extends VulcanValueEnum[Long, EntryType] {
  this: ValueEnum[Long, EntryType] =>

  given vulcanCodec(using names: Names[EntryType]): Codec.Aux[Avro.Long, EntryType] =
    Vulcan.codec(this)
}

trait ShortVulcanEnum[EntryType <: ShortEnumEntry] extends VulcanValueEnum[Short, EntryType] {
  this: ValueEnum[Short, EntryType] =>

  given vulcanCodec(using names: Names[EntryType]): Codec.Aux[Avro.Int, EntryType] =
    Vulcan.codec(this)
}

trait StringVulcanEnum[EntryType <: StringEnumEntry] extends VulcanValueEnum[String, EntryType] {
  this: ValueEnum[String, EntryType] =>

  given vulcanCodec(using names: Names[EntryType]): Codec.Aux[Avro.EnumSymbol, EntryType] =
    Vulcan.enumCodec(this)
}
