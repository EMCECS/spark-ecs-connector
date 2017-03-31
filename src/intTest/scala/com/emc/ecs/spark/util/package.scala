package com.emc.ecs.spark

import java.sql.Timestamp
import java.time.Instant

package object util {

  /**
    * Implicit conversion from a string literal to a [[Timestamp]].
    *
    * {{{
    *   import java.sql.Timestamp
    *   import com.emc.ecs.spark.util._
    *   val time: Timestamp = t"2007-01-01T00:00:00.00Z"
    * }}}
    */
  implicit class StringTimeFunctions(sc: StringContext) {
    def t(args: Any*): Timestamp = Timestamp.from(Instant.parse(sc.s(args:_*)))
  }
}
