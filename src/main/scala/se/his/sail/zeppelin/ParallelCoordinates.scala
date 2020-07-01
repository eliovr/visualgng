package se.his.sail.zeppelin

import org.apache.zeppelin.display.angular.paragraphscope._
import AngularElem._
import se.his.sail.common.{FeaturesSummary, Utils}

import scala.xml.Elem

class ParallelCoordinates private (val id: String, val dataHub: DataHub) {

  private var features: FeaturesSummary = _

  def setFeatures(features: FeaturesSummary): ParallelCoordinates = {
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
         |var $id = new ParallelCoordinates('$id', ${this.features.toJSON});
         |$id.listen(${dataHub.id});
         |${dataHub.id}.notify($id);
         |""".stripMargin)

    <div>
      <svg id={this.id}></svg>
      <script> { script } </script>
    </div>
  }

  /**
    * Display element in Zeppelin.
    * */
  def display: this.type = {
    this.elem.display()
    this
  }
}

object ParallelCoordinates {
  private var idCounter = -1

  private def initialize(): Unit = {
    val script = Utils.getResource("js/pc.js").getLines().mkString("\n")
    <script> { new ScriptText(script) } </script>.display()
  }

  def apply(): ParallelCoordinates = {
    apply(DataHub())
  }

  def apply(hub: DataHub): ParallelCoordinates = {
    if (idCounter < 0) { initialize() }
    new ParallelCoordinates(this.nextId, hub)
  }

  /**
    * Next ID for a Parallel Coordinates object.
    * */
  private def nextId: String = {
    idCounter += 1
    s"pc_$idCounter"
  }
}

