package se.his.sail.zeppelin

import org.apache.zeppelin.display.angular.paragraphscope._
import AngularElem._
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import se.his.sail.Utils._

import scala.xml.Elem

class ParallelCoordinates {

  val id: String = ParallelCoordinates.nextID

  /**
    * ID of the HTML element where data is to be "dropped".
    * */
  private val dataBucketId = id + "_bucket"

  /**
    * HTML element where data is to be "dropped".
    * */
  private val dataBucket =
    <div id={dataBucketId} style="display:none;">
      {{{{{dataBucketId}}}}}
    </div>.model(dataBucketId, "")

  this.dataBucket.display()

  private var featureNames: Array[String] = Array.empty

  /**
    * Stats of each column.
    * These are used for taking the minimum and maximum values in order
    * to scale from value to pixels.
    * */
  var stats: Option[MultivariateStatisticalSummary] = None


  /**
    * Javascript code to be called when the plot is displayed.
    * */
  private var onDisplayScript: String = ""

  /**
    * Javascript code to be called when a PC filter is dragged.
    * */
  private var onFilterScript: String = ""

  /**
    * Hue/color of the lines.
    * */
  private var lineHue = "gray"

  private var colorBy: Option[Int] = Some(0)

  /**
    * Width of the lines.
    * */
  private var lineWidth = .5

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
    * Sets the color of the lines (data points) in the plot.
    * Must be set before calling push().
    *
    * @param hue CSS color of the lines (e.g. red, #ccc). Default is gray.
    * */
  def setLineHue(hue: String): ParallelCoordinates = {
    this.lineHue = hue
    this
  }

  /**
    * Sets the width of the lines (data points) in the plot.
    * Must be set before calling push().
    *
    * @param width Width in pixels. Default is 0.5 px.
    * */
  def setLineWidth(width: Double): ParallelCoordinates = {
    this.lineWidth = width
    this
  }

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

    this.displayed = true

    <div id={id}>
      <svg></svg>
      <script>
        {new ScriptText(jsScript)}
      </script>
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

  /**
    * Set a Javascript code to be executed when the plot is displayed.
    * Has to be set before accessing/displaying the PC element.
    * */
  def setOnDisplayScript(script: String): this.type = {
    this.onDisplayScript = script
    this
  }

  /**
    * Set a Javascript code to be executed when a PC filter is dragged.
    * Has to be set before accessing/displaying the PC element.
    * */
  def setOnFilterScript(script: String): this.type = {
    this.onFilterScript = script
    this
  }

  def scriptSetData(varName: String): String =
    s"$id.setData($varName);"

  private def jsScript: String = {
    val stats = this.stats.get

    val features = (this.featureNames, stats.max.toArray, stats.min.toArray)
      .zipped
      .map((name, max, min) => s"{name: '$name', min: $min, max: $max}")
      .mkString("[", ",", "]")

    getResource("js/pc.js").format(Map(
      "$id" -> this.id,
      "$dataBucketId" -> this.dataBucketId,
      "$features" -> features,
      "$onFilterScript" -> this.onFilterScript,
      "$onDisplayScript" -> this.onDisplayScript
    ))
  }
}

object ParallelCoordinates {
  private var idCounter = 0

  /**
    * Next ID for a Parallel Coordinates object.
    * */
  private def nextID: String = {
    idCounter += 1
    s"pc_$idCounter"
  }
}

