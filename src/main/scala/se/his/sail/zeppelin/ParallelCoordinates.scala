package se.his.sail.zeppelin

import org.apache.zeppelin.display.angular.paragraphscope._
import AngularElem._
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import se.his.sail.Utils
import se.his.sail.Utils._

import scala.xml.Elem

class ParallelCoordinates private (val id: String, val dataHub: DataHub) {

  private var featureNames: Array[String] = Array.empty

  /**
    * Stats of each column.
    * These are used for taking the minimum and maximum values in order
    * to scale from value to pixels.
    * */
  var stats: Option[MultivariateStatisticalSummary] = None


  private var colorBy: Option[Int] = Some(0)

  private var displayed = false

  def setFeatureNames(names: Array[String]): ParallelCoordinates = {
    this.featureNames = names
    this
  }

  def setStats(stats: MultivariateStatisticalSummary): ParallelCoordinates = {
    this.stats = Option(stats)
    this
  }

  def isDisplayed: Boolean = this.displayed


  /**
    * Set a data feature by which the lines will be colored.
    * Color is based on Spence et al. (2001) where orange is high values
    * pink low and gray average.
    * */
  def colorBy(feature: String): ParallelCoordinates = {
    val index = this.featureNames.indexWhere(_ == feature)

    if (index >= 0) this.colorBy = Some(index)
    else this.colorBy = None

    this
  }


  /**
    * The HTML force directed graph element.
    * */
  lazy val elem: Elem = {
    require(this.featureNames.nonEmpty, "Feature names cannot be empty")
    require(this.stats.nonEmpty, "Stats (MultivariateStatisticalSummary of the features) must be set")

    val stats = this.stats.get

    val features = (this.featureNames, stats.max.toArray, stats.min.toArray)
      .zipped
      .map((name, max, min) => s"{name: '$name', min: $min, max: $max}")
      .mkString("[", ",", "]")

    val script = new ScriptText(
      s"""
         |var $id = new ParallelCoordinates('$id');
         |$id.setFeatures($features);
         |$id.listen(${dataHub.id});
         |${dataHub.id}.notify($id);
         |""".stripMargin)

    this.displayed = true

    <div>
      <svg id={this.id}></svg>
      <script> { script } </script>
    </div>
  }

  /**
    * Display element in Zeppelin.
    * */
  def display(): this.type = {
    this.elem.display()
    this.displayed = true
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

