package org.apache.spark.sql.columnar.hotfoot

import com.guavus.hotfoot.util.random._
import org.apache.spark.sql.types.{AtomicType, Metadata}

/**
 * Created by yash.datta on 01/03/16.
 */

import com.guavus.hotfoot.util.random.Dist._

class RandomIntGenerator(properties: Metadata = Metadata.empty, dist: Dist = UniformDist)
  extends IntColumnGenerator {

  var start: Int = 0
  var end: Int = 0
  var cardinality: Int = 0

  val DEFAULT_CARDINAILTY: Int = 100

  val distribution = dist match {
    case UniformDist => {
      // extract start, end, cardinality from properties
      if (properties.contains("start")) {
        start = properties.getLong("start").asInstanceOf[Int]
      }
      if (properties.contains("end")) {
        end = properties.getLong("end").asInstanceOf[Int]
      }
      // TODO: Make use of cardinality while generating values
      if (properties.contains("cardinality")) {
        cardinality = properties.getLong("cardinality").asInstanceOf[Int]
      }

      end match {
        case 0 =>
          end = start + (if (cardinality == 0) DEFAULT_CARDINAILTY else cardinality)
        case _ =>
          if (cardinality != 0) {
            // still to handle this case
          }
      }
      // Make sure start, end values are good
      assert(end > start)
      new UniformDistribution(start, end)
    }
    case PoissonDist => new PoissonDistribution
    case XORShiftUniformDist => new XORShiftUniformDistribution
    case _ => new Distribution
  }

  // Iterator to generate random values
  val nums = new Iterator[Int] {
    def hasNext() = true
    def next(): Int = distribution.nextInt
  }

  override def generate(): Int = {
    nums.next
  }
}
