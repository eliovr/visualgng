package se.his.sail.common

import scala.collection.mutable

case class JSONObject() {
  private val attributes: mutable.Map[String, Any] = mutable.Map()

  def setAttr(k: String, v: Any): this.type = {
    attributes += k -> v
    this
  }

  override def toString: String = {
    attributes
      .map{
        case (k, v: Double) => s""""$k": $v"""
        case (k, v: Int) => s""""$k": $v"""
        case (k, v: Long) => s""""$k": $v"""
        case (k, v: JSONObject) => s""""$k": $v"""
        case (k, v: JSONArray) => s""""$k": $v"""
        case (k, v) => s""""$k": "${v.toString}""""
      }
      .mkString("{", ", ", "}")
  }
}

case class JSONArray(values: Iterable[_]) {
  override def toString: String = values.mkString("[", ", ", "]")
}