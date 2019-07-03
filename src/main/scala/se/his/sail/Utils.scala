package se.his.sail

import breeze.linalg._

import scala.collection.mutable
import scala.io.Source

object Utils {

  /**
    * Minkowski distance between two vectors.
    * */
  def minkowski(p: Double)(a: DenseVector[Double], b: DenseVector[Double]): Double =
    norm(a - b, p)

  def pearson(a: DenseVector[Double], b: DenseVector[Double]): Double = {
    val m1 = norm(a, 1) / a.length
    val m2 = norm(b, 1) / b.length

    val d1 = a - m1
    val d2 = b - m2

    norm(d1 *:* d2, 1) / (norm(d1) * norm(d2))
  }

  /**
    * Scale a value from a given range (max - min) to another (b - a).
    * @param min Minimum value that x can take.
    * @param max Maximum value that x can take.
    * @param a Minimum value for the scaled value.
    * @param b Maximum value for the scaled value.
    * @param x Value to be scaled.
    * */
  def scale(min: Double, max: Double)(a: Double, b: Double)(x: Double): Double = {
    val range = max - min

    if (range > 0) (((b - a) * (x - min)) / range) + a
    else 0
  }

  /**
    * Returns the amount of seconds it takes to execute f.
    * */
  def performance(f: => Unit): Double = {
    val start = System.nanoTime()
    f
    (System.nanoTime() - start) / 1000000000
  }

  def spenceHSL(stats: Stats)(x: Double): (Double, Double, Double) = {
    val lightness = .78 - scale(stats.min, stats.max)(.59, .78)(x) + .59

    val saturation =
      if (stats.min >= 0) {
        val avg = stats.avg
        if (x > avg)
          scale(avg, stats.max)(0, .95)(x)
        else
          scale(0, avg - stats.min)(0, .95)(avg - x)
      }
      else if (x < 0)
        scale(0, stats.min.abs)(0, .95)(x.abs)
      else
        scale(0, stats.max)(0, .95)(x)

    val hue =
      if (x < stats.avg) 346
      else if (x > stats.avg) 34
      else 0

    (hue, saturation, lightness)
  }

  /***
    * Retrieve a file from the "resource" folder in the project.
    */
  def getResource(resource: String): Source =
    Source.fromInputStream(getClass.getResourceAsStream("/" + resource))

  /**
    * Format a number to a short string version e.g. 100.000 to 100K.
    * */
  def toShortString(n: Long): String = {
    val suffixes: mutable.Map[Double, String] = mutable.Map(
      1e18 -> "E",
      1e15 -> "P",
      1e12 -> "T",
      1e9 -> "G",
      1e6 -> "M",
      1e3 -> "K",
      1.0 -> ""
    )

    suffixes.find(n >= _._1) match {
      case Some((v, s)) =>
        if (n % v == 0) (n / v).toInt + s
        else "%1.1f".format(n / v) + s
      case None => n + ""
    }
  }

  implicit def sourceAsWrappedSource(source: Source): WrappedSource = new WrappedSource(source)
}



private [sail] class WrappedSource (val source: Source) {
  def format(values: Map[String, Any]): String = {
    var str = source.getLines().mkString("\n")
    values.foreach{case (k, v) => str = str.replace(k, v.toString)}
    str
  }
}

case class Stats(
             count: Long = 1,
             sum: Double = 0,
             min: Double = Double.MaxValue,
             max: Double = Double.MinValue) extends Serializable {

  def +(other: Stats): Stats = other match {
    case Stats(_, s, mn, mx) => Stats(count + 1, sum + s, mn.min(min), mx.max(max))
  }

  def +(x: Double): Stats = Stats(count + 1, sum + x, x.min(min), x.max(max))

  def avg: Double = sum / count
}