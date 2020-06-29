package se.his.sail.ml

import breeze.{linalg => br}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.linalg.{Vector => SparkVector}
import org.apache.spark.sql.functions.udf

import scala.collection.mutable.ArrayBuffer

class Node(
            val id: Int,
            val prototype: br.DenseVector[Double],
            var error: Double = 0,
            var utility: Double = 0,
            var winCounter: Long = 0
          ) extends Serializable {

  var label: Option[String] = None

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
            var age: Double = 0
          ) extends Serializable {

  def connects(u: Node): Boolean = target.id == u.id || source.id == u.id

  def connects(a: Node, b: Node): Boolean = connects(a) && connects(b)

  def getPartner(u: Node): Node =
    if (source.id == u.id) target
    else source

  def distance: Double = source.distanceTo(target.prototype)

}


class GNGModel private () extends Serializable {

  var nodes: ArrayBuffer[Node] = ArrayBuffer.empty
  var edges: ArrayBuffer[Edge] = ArrayBuffer.empty

  var nextUnitId: Int = 0
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
    val u = new Node(nextUnitId, prototype)
    nodes.append(u)
    nextUnitId += 1
    u
  }

  def transform(ds: Dataset[_]): Dataset[_] = {
//    val prototypes: br.DenseMatrix[Double] = br.DenseMatrix(nodes.map(_.prototype):_*)
    val classify = udf{(col: SparkVector) =>
      val vec = br.DenseVector(col.toArray)
//      val diff: br.DenseMatrix[Double] = vec - prototypes(br.*, ::)
//      br.argmin(br.norm(diff(br.*, ::)))

      nodes.zipWithIndex.minBy{ case (n, _) => n.distanceTo(vec) }._2
//      nodes.minBy(_.distanceTo(vec)).id
    }

    ds.withColumn(outputCol, classify(ds(inputCol)))
  }
}


object GNGModel {
  def apply(rdd: RDD[br.DenseVector[Double]]): GNGModel = {
    val samples = rdd.takeSample(false, 2)
    val model = new GNGModel
    val a = model.createUnit(samples(0))
    val b = model.createUnit(samples(1))
    model.edges.append(new Edge(a, b))

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


