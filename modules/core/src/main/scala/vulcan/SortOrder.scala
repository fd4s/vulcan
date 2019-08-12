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

package vulcan

import org.apache.avro.Schema

/**
  * How field values should be ordered when sorting records.
  *
  * The following sort orders are available.<br>
  * - [[SortOrder.Ascending]] for ascending sort order,<br>
  * - [[SortOrder.Descending]] for descending sort order,<br>
  * - [[SortOrder.Ignore]] to ignore the field when sorting.
  */
sealed abstract class SortOrder {
  def asJava: Schema.Field.Order
}

final object SortOrder {
  private[vulcan] final case object AscendingSortOrder extends SortOrder {
    override final val asJava: Schema.Field.Order =
      Schema.Field.Order.ASCENDING

    override final def toString: String =
      "Ascending"
  }

  private[vulcan] final case object DescendingSortOrder extends SortOrder {
    override final val asJava: Schema.Field.Order =
      Schema.Field.Order.DESCENDING

    override final def toString: String =
      "Descending"
  }

  private[vulcan] final case object IgnoreSortOrder extends SortOrder {
    override final val asJava: Schema.Field.Order =
      Schema.Field.Order.IGNORE

    override final def toString: String =
      "Ignore"
  }

  final val Ascending: SortOrder =
    AscendingSortOrder

  final val Descending: SortOrder =
    DescendingSortOrder

  final val Ignore: SortOrder =
    IgnoreSortOrder
}
