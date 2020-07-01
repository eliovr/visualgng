package se.his.sail.zeppelin

import org.apache.zeppelin.display.angular.notebookscope._
import AngularElem._
import se.his.sail.common.Utils

class DataHub private (val id: String) {
  private val inputBucketId = s"${id}_input_bucket"
  private val inputBucket =
    <div id={inputBucketId} style="display:none;">
      {{{{{inputBucketId}}}}}
    </div>.model(inputBucketId, "")
  this.inputBucket.display()

  private val outputBucketId = s"${id}_output_bucket"
  private val outputBucket =
    <div id={outputBucketId} style="display:none;">
      {{{{{outputBucketId}}}}}
    </div>.model(outputBucketId, "")
  this.outputBucket.display()

  private val script: ScriptText = {
    new ScriptText(
      s"""
        |var $id = new DataHub('$id', '$inputBucketId', '$outputBucketId');
        |""".stripMargin)
  }

  <div style="display:none;">
    {inputBucket}
    {outputBucket}
    <script>{ script }</script>
  </div>.display()

  def put(jsonData: String): this.type = {
    this.inputBucket.model(this.inputBucketId, jsonData)
    this
  }

  def get: String = {
    AngularModel(outputBucketId)().asInstanceOf[String]
  }
}

object DataHub {
  private var idCounter: Int = -1
  private val resource: String = "js/datahub.js"
  private val prefix: String = "hub"

  private def nextId: String = {
    this.idCounter += 1
    prefix + "_" + idCounter
  }

  private def initialize(): Unit = {
    val script = Utils.getResource(resource).getLines().mkString("\n")
    <script> { new ScriptText(script) } </script>.display()
  }

  def apply(): DataHub = {
    if (idCounter < 0) { initialize() }
    new DataHub(this.nextId)
  }
}
