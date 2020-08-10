package se.his.sail.ml

import breeze.{linalg => br}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.linalg.{Vector => SparkVector}
import org.apache.spark.sql.functions.udf
import se.his.sail.ml.GNGModel.Prediction

import scala.collection.mutable.ArrayBuffer

class Node( val id: Int,
            val prototype: br.DenseVector[Double],
            var error: Double,
            var utility: Double,
            var winCounter: Long
          ) extends Serializable {

  def this(id: Int, prototype: br.DenseVector[Double]) = this(id, prototype, 0, 0, 0)

  var label: Option[String] = None
  var certainty: Double = 0

  def setLabel(label: String): this.type = {
    this.label = Some(label)
    this
  }

  def moveTowards(vector: br.DenseVector[Double], eps: Double): br.DenseVector[Double] = {
    prototype :+= (vector - prototype) * eps
  }

  def distanceTo(vector: br.DenseVector[Double], p: Double = 2): Double = {
    br.norm(prototype - vector, p)
  }
}

class Edge(
            val source: Node,
            val target: Node,
            var age: Double = 0,
            var maxAge: Double = 25
          ) extends Serializable {

  def connects(u: Node): Boolean = target.id == u.id || source.id == u.id

  def connects(a: Node, b: Node): Boolean = connects(a) && connects(b)

  def getPartnerOf(u: Node): Node =
    if (source.id == u.id) target
    else source

  def distance: Double = source.distanceTo(target.prototype)

}


class GNGModel private () extends Serializable {

  var nodes: ArrayBuffer[Node] = ArrayBuffer.empty
  var edges: ArrayBuffer[Edge] = ArrayBuffer.empty

  var nextUnitId: Int = -1
  var inputCol: String = "features"
  var outputCol: String = "prediction"

  def setInputCol(col: String): GNGModel = {
    this.inputCol = col
    this
  }

  def setOutputCol(col: String): GNGModel = {
    this.outputCol = col
    this
  }

  def getInputCol: String = this.inputCol
  def getOutputCol: String = this.outputCol

  def createUnit(prototype: br.DenseVector[Double]): Node = {
    nextUnitId += 1
    val unit = new Node(nextUnitId, prototype)
    nodes.append(unit)
    unit
  }

  def transform(ds: Dataset[_], withDistance: Boolean = false): Dataset[_] = {
    val classify = if (withDistance) {
      udf((col: SparkVector) => {
        val vec = br.DenseVector(col.toArray)
        nodes.zipWithIndex
          .map{ case (n, i) => Prediction(i, n.distanceTo(vec)) }
          .minBy(_.distance)
      })
    } else {
      udf((col: SparkVector) => {
        val vec = br.DenseVector(col.toArray)
        nodes.zipWithIndex
          .minBy{ case (n, _) => n.distanceTo(vec) }
          ._2
      })
    }

    ds.withColumn(outputCol, classify(ds(inputCol)))
  }
}

case object GNGModel {
  case class Prediction (unitId: Int, distance: Double)

  def apply(rdd: RDD[br.DenseVector[Double]], maxAge: Double): GNGModel = {
    val samples = rdd.takeSample(false, 2)
    val model = new GNGModel
    val a = model.createUnit(samples(0))
    val b = model.createUnit(samples(1))
    model.edges.append(new Edge(a, b, maxAge=maxAge))

    model
  }

  def apply(samples: Seq[br.DenseVector[Double]]): GNGModel = {
    val size = samples.size
    val model = new GNGModel
    val a = model.createUnit(samples(size - 1))
    val b = model.createUnit(samples(size - 2))
    model.edges.append(new Edge(a, b))

    model
  }
}


