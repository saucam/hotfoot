package com.guavus.hotfoot

/**
 * Created by yash.datta on 02/07/15.
 */
package object columnar {
  /**
   * A JVM-global lock that should be used to prevent thread safety issues when using things in
   * scala.reflect.*.  Note that Scala Reflection API is made thread-safe in 2.11, but not yet for
   * 2.10.* builds.  See SI-6240 for more details.
   */
  protected[hotfoot] object ScalaReflectionLock
}
