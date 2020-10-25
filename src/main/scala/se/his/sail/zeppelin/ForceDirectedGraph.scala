package se.his.sail.zeppelin

import org.apache.zeppelin.display.angular.notebookscope._
import AngularElem._
import se.his.sail.common.{FeaturesSummary, Utils}

import scala.collection.mutable
import scala.xml.Elem

class ForceDirectedGraph private (val id: String, val dataHub: DataHub) {

  private var features: FeaturesSummary = _

  def setFeatures(features: FeaturesSummary): this.type = {
    this.features = features
    this
  }

  /**
    * The HTML force directed graph element.
    * */
  lazy val elem: Elem = {
    require(this.features != null, "Feature cannot be null")

    val script = new ScriptText(
      s"""
         |var $id = new ForceDirectedGraph('$id', ${this.features.toJSON});
         |$id.listen(${dataHub.id});
         |${dataHub.id}.notify($id);
         |""".stripMargin)

    <div>
      <div id={this.id}></div>
      <script> { script } </script>
    </div>
  }

  /**
    * Set and display nodes and edges on the force directed graph.
    * It replaces previous nodes and edges.
    * */
  def setData(nodes: Iterable[GraphNode], edges: Iterable[GraphEdge]): this.type = {
    val jsonNodes = nodes.mkString("[", ",", "]")
    val jsonEdges = edges.mkString("[", ",", "]")
    val data = s"""{"nodes": $jsonNodes, "links": $jsonEdges}"""

    dataHub.push(data)
    this
  }
}

object ForceDirectedGraph {
  private var idCounter = -1

  private def initialize(): Unit = {
    val script = Utils.getResource("js/fdg.js").getLines().mkString("\n")
    <script> { new ScriptText(script) } </script>.display()
  }

  /**
    * Next ID for a Parallel Coordinates object.
    * */
  private def nextId: String = {
    idCounter += 1
    s"fdg_$idCounter"
  }

  def apply(): ForceDirectedGraph = {
    this.apply(DataHub())
  }

  def apply(hub: DataHub): ForceDirectedGraph = {
    if (idCounter < 0) { initialize() }
    new ForceDirectedGraph(this.nextId, hub)
  }
}

/** Represents a node in the graph. */
class GraphNode(val id: Int, radius: Double = 5) {
  private val attr: mutable.Map[String, String] = mutable.Map(
    "id" -> id.toString,
    "radius" -> radius.toString,
    "selected" -> "false")

  def setRadius(r: Double): GraphNode = {
    attr += "radius" -> r.toString
    this
  }

  def setHSL(h: Int, s: Double, l: Double): GraphNode = {
    attr += "hsl" -> s"[$h, $s, $l]"
    this
  }

  def setHue(h: Int): GraphNode = {
    attr += "hue" -> h.toString
    this
  }

  def setGroup(g: Int): GraphNode = {
    attr += "group" -> g.toString
    this
  }

  def setData(data: String): GraphNode = {
    attr += "data" -> data
    this
  }

  def setHint(hint: String): GraphNode = {
    attr += "hint" -> ("\"" + hint + "\"")
    this
  }

  def setImage(url: String): GraphNode = {
    attr += "img" -> ("\"" + url + "\"")
    this
  }

  override def toString: String = attr
    .map{case (k, v) => "\"" + k + "\": " + v}
    .mkString("{", ", ", "}")
}

/** Represents an edge in the graph.
  * @param source Source node id.
  * @param target Target node id.
  * @param distance Edge length i.e. distance between nodes.
  * */
class GraphEdge(val source: Int, val target: Int, val distance: Double = 10) {
  private val attr: mutable.Map[String, String] = mutable.Map(
    "source" -> source.toString,
    "target" -> target.toString,
    "distance" -> distance.toString
  )

  def setOpacity(o: Double): GraphEdge= {
    attr += "opacity" -> o.toString
    this
  }

  def setWidth(w: Double): GraphEdge= {
    attr += "width" -> w.toString
    this
  }

  def setHSL(h: Int, s: Double, l: Double): GraphEdge= {
    attr += "hsl" -> s"[$h, $s, $l]"
    this
  }

  def setHue(color: String): GraphEdge= {
    attr += "hue" -> color
    this
  }

  override def toString: String = attr
    .map{case (k, v) => "\"" + k + "\": " + v}
    .mkString("{", ", ", "}")
}
