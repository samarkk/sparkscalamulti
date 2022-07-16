package core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

class MyComplex(var x: Int, var y: Int) extends Serializable {
  def reset(): Unit = {
    x = 0
    y = 0
  }
  def add(p: MyComplex): MyComplex = {
    x = x + p.x
    y = y + p.y
    return this
  }
}

object ComplexAccumulatorV2 extends AccumulatorV2[MyComplex, MyComplex] {
  private val myc: MyComplex = new MyComplex(0, 0)

  def reset(): Unit = {
    myc.reset()
  }

  def add(v: MyComplex): Unit = {
    myc.add(v)
  }
  def value(): MyComplex = {
    return myc
  }
  def isZero(): Boolean = {
    return (myc.x == 0 && myc.y == 0)
  }
  def copy(): AccumulatorV2[MyComplex, MyComplex] = {
    return ComplexAccumulatorV2
  }
  def merge(other: AccumulatorV2[MyComplex, MyComplex]) = {
    myc.add(other.value)
  }
}

object CAccumMain extends App {
  val conf = new SparkConf
  conf.setAppName("SparkComplexAccumulatorExample")
//  conf.setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.register(ComplexAccumulatorV2, "mycomplexacc")

  //using custom accumulator

  var ca = ComplexAccumulatorV2

  var rdd = sc.parallelize(1 to 10)
  var res = rdd.map(x => ca.add(new MyComplex(x, x)))
  println("res count: " + res.count)
  println("ca.value.x: " + ca.value.x)
  println("ca.value.y: " + ca.value.y)
}
