package se.his.sail.common

import java.awt.image.BufferedImage

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
    (System.nanoTime() - start) / 1e9
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

  def createImage(pixels: Matrix[Int], saveTo: String, format: String = "png"): Unit = {
    val h = pixels.rows
    val w = pixels.cols
    val imgType = BufferedImage.TYPE_BYTE_GRAY
    val image = new BufferedImage(w, h, imgType)

    for (y <- 0 until h) {
      for (x <- 0 until w) {
        image.setRGB(x, y, pixels(x, y))
      }
    }

    javax.imageio.ImageIO.write(image, format, new java.io.File(saveTo))
  }
}

