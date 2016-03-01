package com.guavus.hotfoot.util.random

import org.apache.commons.math3
import org.apache.commons.math3.distribution.UniformIntegerDistribution

import scala.util.Random

/**
 * Created by yash.datta on 01/03/16.
 */
object Dist extends Enumeration {
  type Dist = Value
  val UniformDist = Value("UniformDistribution")
  val PoissonDist = Value("PoissonDistribution")
  val XORShiftUniformDist = Value("XORShiftUniformDistribution")
  val SimpleDist = Value("SimpleRandomDistribution")
}

class Distribution {

  private val random = new Random

  def nextInt: Int = random.nextInt
  def nextDouble: Double = random.nextDouble
}

class UniformDistribution(lower: Int, upper: Int) extends Distribution {
  private val uniformRandom = new UniformIntegerDistribution(lower, upper)

  override def nextInt: Int = {
    uniformRandom.sample
  }
}

class XORShiftUniformDistribution extends Distribution {
  val seed = 1L

  private val xorRandom = new XORShiftRandom(seed)

  override def nextInt: Int = {
    xorRandom.nextInt
  }
}

class PoissonDistribution (mean: Int = 100) extends Distribution {
  private val poissonRandom = new math3.distribution.PoissonDistribution(mean)
  private var poissonSeed = 0L

  poissonRandom.reseedRandomGenerator(poissonSeed)

  override def nextInt: Int = poissonRandom.sample
}
