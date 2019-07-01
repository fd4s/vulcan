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

/**
  * `Throwable` representation of an [[AvroError]],
  * created with [[AvroError#throwable]].
  */
sealed abstract class AvroException(message: String) extends RuntimeException(message)

private[vulcan] final object AvroException {
  final def apply(message: String): AvroException =
    new AvroException(message) {
      override final def toString: String =
        s"vulcan.AvroException: $getMessage"
    }
}
