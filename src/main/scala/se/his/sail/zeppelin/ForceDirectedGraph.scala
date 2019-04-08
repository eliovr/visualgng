package se.his.sail.zeppelin

import org.apache.zeppelin.display.angular.notebookscope._
import AngularElem._
import se.his.sail.Utils._

import scala.collection.mutable
import scala.xml.Elem

class ForceDirectedGraph (height: Int = 600, width: Int = 600) {

  val id: String = ForceDirectedGraph.nextID

  private val events: mutable.Map[String, String] = mutable.Map(
    "onmouseover" -> "",
    "onmouseout" -> "",
    "onclicked" -> "",
    "onupdate" -> "")

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
    <div id={this.id} style={s"min-width: ${width}px;"}>
      <svg height={height.toString} width={width.toString}>
        <g class="links" style="stroke: black; stroke-opacity: .5; stroke-width: .5"></g>
        <g class="nodes" style="stroke: #fff; stroke-width: 2px; fill: lightgray;"></g>
      </svg>
      <script> {new ScriptText(script)} </script>
    </div>
  }

  /**
    * Set and display nodes and edges on the force directed graph.
    * It replaces previous nodes and edges.
    * */
  def setData(nodes: List[GraphNode], edges: List[GraphEdge]): this.type = {
    val jsonNodes = nodes.mkString(",")
    val jsonEdges = edges.mkString(",")
    val data = "{\"nodes\": [" + jsonNodes + "], \"links\": [" + jsonEdges + "]}"

    // push data to the html bucket element
    this.dataBucket.model(this.dataBucketId, data)
    this
  }

  def setClickedScript(script: String): this.type = {
    this.events += "onclicked" -> script
    this
  }

  def setMouseoverScript(script: String): this.type = {
    this.events += "onmouseover" -> script
    this
  }

  def setMouseoutScript(script: String): this.type = {
    this.events += "onmouseout" -> script
    this
  }

  def setOnUpdateScript(script: String): this.type = {
    this.events += "onupdate" -> script
    this
  }

  def scriptSetData(nodes: String, links: String): String =
    s"fdgSetData($id, $nodes, $links)"

  /**
    * Javascript code which handles the the force directed graph.
    * */
  private def script: String = {
    getResource("js/fdg.js").format(Map(
      "$id" -> this.id,
      "$dataBucketId" -> this.dataBucketId,
      "$width" -> width,
      "$height" -> height,
      "$onMouseover" -> this.events("onmouseover"),
      "$onMouseout" -> this.events("onmouseout"),
      "$onClicked" -> this.events("onclicked"),
      "$onUpdate" -> this.events("onupdate")
    ))
  }

}

object ForceDirectedGraph {
  private var idCounter = 0

  /**
    * Next ID for a Parallel Coordinates object.
    * */
  private def nextID: String = {
    idCounter += 1
    s"fdg_$idCounter"
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
