package se.his.sail.zeppelin

import org.apache.zeppelin.display.angular.notebookscope._
import AngularElem._
import se.his.sail.Utils

import scala.collection.mutable
import scala.xml.Elem

class ForceDirectedGraph private (val id: String) {

  var height: Int = 600
  var width: Int = 600
  var minNodeRadius: Int = 5
  var maxNodeRadius: Int = 15
  var maxEdgeDistance: Int = 50

  /**
    * Id for the HTML element where data is to be "dropped".
    * */
  private val dataBucketId = s"${id}_bucket"

  private val dataBucket =
    <div id={dataBucketId} style="display:none;">
      {{{{{dataBucketId}}}}}
    </div>.model(dataBucketId, "")

  this.dataBucket.display()

  /**
    * The HTML force directed graph element.
    * */
  lazy val elem: Elem = {
    val script = new ScriptText(
      s"""
         |var $id = new ForceDirectedGraph('$id', $width, $height);
         |${this.id}.watchBucket('$dataBucketId');
         |""".stripMargin)

    <div style={s"min-width: ${width}px;"}>
      <svg id={this.id}></svg>
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

    // push data to the html bucket element
    this.dataBucket.model(this.dataBucketId, data)
    this
  }

  def addListener(elem: String): this.type = {
    val js = new ScriptText(s"$id.addListener($elem);")
    <script>{ js }</script>.display()
    this
  }

}

object ForceDirectedGraph {
  private var idCounter = 0

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
    if (idCounter == 0) {
      initialize()
    }
    new ForceDirectedGraph(this.nextId)
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
