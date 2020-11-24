package se.his.sail.ml

import breeze.{linalg => br}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.linalg.{Vector => SparkVector}
import org.apache.spark.sql.functions.udf
import se.his.sail.ml.GNGModel.Prediction

import scala.collection.mutable.ArrayBuffer
import java.io._

import se.his.sail.common.{JSONArray, JSONObject, Utils}

import scala.collection.mutable

class Node( val id: Int,
            val prototype: br.DenseVector[Double],
            var error: Double,
            var utility: Double,
            var winCounter: Long
          ) extends Serializable {

  def this(id: Int, prototype: br.DenseVector[Double]) = this(id, prototype, 0, 0, 0)

  var label: Option[String] = None
  var group: Option[String] = None
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
            val target: Node
          ) extends Serializable {

  var age: Double = 0

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
  var outputCol: String = "gng"

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
          .map{ case (n, i) => Prediction(i, n.distanceTo(vec), n.group.orNull, n.label.orNull) }
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

  def toJSONString: String = {
    val labels: mutable.Map[String, Int] = mutable.Map.empty

    val nodes: Iterable[JSONObject] = this.nodes.zipWithIndex.map{case (n, i) =>
      val obj = JSONObject()
        .setAttr("id", n.id)
        .setAttr("trueId", i)
        .setAttr("density", n.winCounter)

      n.label match {
        case Some(x) =>
          obj
            .setAttr("hint", s"$i: $x")
            .setAttr("group", labels.getOrElseUpdate(x.toString, labels.size))
            .setAttr("data", JSONArray(n.certainty +: n.prototype.toArray))
        case None =>
          obj
            .setAttr("hint", s"$i")
            .setAttr("data", JSONArray(n.prototype.toArray))
      }
    }

    val edges: Iterable[JSONObject] = this.edges.map{e =>
      JSONObject()
        .setAttr("source", this.nodes.indexWhere(_.id == e.source.id))
        .setAttr("target", this.nodes.indexWhere(_.id == e.target.id))
        .setAttr("distance", e.distance)
    }

    JSONObject()
      .setAttr("nodes", JSONArray(nodes))
      .setAttr("links", JSONArray(edges))
      .toString
  }

  def toGMLString: String = {
    val nodes: Iterable[String] = this.nodes.zipWithIndex.map{case (n, i) =>
        s"""
         |node
         |[
         | id $i
         | label "$i"
         | density ${n.winCounter}
         | group ${n.label.getOrElse(0)}
         |]
         |""".stripMargin
    }

    val edges : Iterable[String] = this.edges.map{e =>
      s"""
         |edge
         |[
         | source ${this.nodes.indexWhere(_.id == e.source.id)}
         | target ${this.nodes.indexWhere(_.id == e.target.id)}
         | distance ${e.distance}
         |]
         |""".stripMargin
    }

    s"""
       |graph
       |[
       | ${nodes.mkString("")}
       | ${edges.mkString("")}
       |]
       |""".stripMargin
  }


  def saveAsImages(imgWidth: Int, imgHeight: Int, channels: Int, folder: String): Unit = {
    nodes.zipWithIndex.foreach{ case (u, i) =>
      val w = imgWidth * channels
      val h = imgHeight
      //    val pixels = br.convert(u.prototype.asDenseMatrix.reshape(h, w) * 255.0, Int)
      val pixels = br.convert(u.prototype.asDenseMatrix.reshape(h, w), Int)
      Utils.createImage(pixels, channels, s"$folder/$i.png")
    }
  }

  /**
    * Saves the model as JSON. This includes the prototypes.
    * @param filePath full path and file name (including extension).
    * */
  def saveAsJSON(filePath: String): Unit = {
    val pw = new PrintWriter(new File(filePath))
    pw.write(this.toJSONString)
    pw.close()
  }

  /**
    * Saves the model as GML (Graph Modeling Language).
    * @param filePath full path and file name (including extension).
    * */
  def saveAsGML(filePath: String): Unit = {
    val pw = new PrintWriter(new File(filePath))
    pw.write(this.toGMLString)
    pw.close()
  }

  /**
    * Saves prototypes as CSV.
    * @param filePath full path and file name (including extension).
    * */
  def savePrototypes(filePath: String): Unit = {
    val prototypes: String = this.nodes
      .map(_.prototype.toArray.mkString(", "))
      .mkString("\n")

    val pw = new PrintWriter(new File(filePath))
    pw.write(prototypes)
    pw.close()
  }
}


case object GNGModel {
  case class Prediction (unitId: Int, distance: Double, group: String, label: String)

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


