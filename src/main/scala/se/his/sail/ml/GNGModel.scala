package se.his.sail.ml

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.ml.linalg.{Vectors, Vector => SparkVector}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import se.his.sail.Utils

/**
  * Representation of a unit/node in the GNG topology.
  * */
class Node private (
            val id: Int,
            val prototype: BDV[Double],
            var error: Double,
            var utility: Double,
            var winCounter: Long,
            var label: Option[Int]
          ) {

  def this(id: Int, prototype: BDV[Double]) = this(id, prototype, 0, 0, 0, None)

  def setLabel(l: Int): Unit = label = Some(l)
  def getLabel: Option[Int] = this.label

  /**
    * (Step 5)
    * Move closer by a fraction (esp_b, esp_n)
    * */
  private[sail] def moveTowards(other: BDV[Double], eps: Double): Unit = {
    prototype += eps * (other - prototype)
  }

  /**
    * Returns the distance of this node's vector to another vector.
    * @return Minkowski distance as given by p.
    * @param vector The other vector from which the distance is to be computed.
    * */
  private[sail] def distanceTo(vector: BDV[Double], p: Double = 1): Double =
    Utils.minkowski(p)(prototype, vector)

}


/**
  * Representation of an edge/link in the GNG topology.
  * */
class Edge(val source: Node, val target: Node) {
  var age: Double = 0.0

  /**
    * Last computed distance between the nodes.
    * Used in graphEdge method in order to avoid recalculation of distance.
    * */
  private var lastDistance: Double = 0.0

  /**
    * Check if n is one the edge's nodes
    * */
  def has(n: Node): Boolean = n.id == source.id || n.id == target.id

  /**
    * Return the other node
    * */
  def getPartner(n: Node): Node =
    if (source == n) target
    else source

  /**
    * Computes the distance between source vector and target vector.
    * */
  def distance(recompute: Boolean = true): Double = {
    if (recompute) {
      lastDistance = source.distanceTo(target.prototype)
    }

    lastDistance
  }
}


class GNGModel(
              private var nodes: List[Node],              // units.
              private var edges: List[Edge],              // connections between units/nodes.
              private var featuresCol: String,
              private var predictionCol: String) {

  def this(nodes: List[Node], edges: List[Edge]) = this(nodes, edges, "features", "prediction")

  def this() = this(Nil, Nil)

  private var nodeId = this.nodes.size

  def createNode(prototype: BDV[Double]): Node = {
    this.nodeId += 1
    new Node(this.nodeId, prototype)
  }

  def getNodeId: Int = this.nodeId

  def setNodeId(start: Int): this.type = {
    this.nodeId = start
    this
  }

  def getNodes: List[Node] = this.nodes

  def getEdges: List[Edge] = this.edges

  def getFeaturesCol: String = featuresCol

  def setFeaturesCol(col: String): this.type = {
    this.featuresCol = col
    this
  }

  def getPredictionCol: String = predictionCol

  def setPredictionCol(col: String): this.type = {
    this.predictionCol = col
    this
  }

  def transform(df: DataFrame, withDistance: Boolean = false): DataFrame = {
    require(this.nodes.size > 1, "The model is empty.")

    val ns = nodes.zipWithIndex
      .map{ case (n, i) => (Vectors.dense(n.prototype.toArray), i) }

    val classifyUDF = if (withDistance) {
      val classifyWithDistance: SparkVector => ClassAndDistance = features =>
        ns
          .map { case (prototype, label) => ClassAndDistance(label, Vectors.sqdist(prototype, features)) }
          .minBy(_.distance)

      udf(classifyWithDistance)
    } else {
      val classify: SparkVector => Int = features =>
        ns.minBy { case (prototype, _) => Vectors.sqdist(prototype, features) }._2

      udf(classify)
    }

//    df.persist()
    df.withColumn(predictionCol, classifyUDF(df(featuresCol)))
  }

}

private[sail] case class ClassAndDistance(label: Int, distance: Double)



