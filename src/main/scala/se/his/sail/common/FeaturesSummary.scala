package se.his.sail.common

case class FeaturesSummary(names: Array[String], min: Array[Double], max: Array[Double]) {
  def toJSON: String = {
    val objects: Iterable[JSONObject] = (names, min, max)
      .zipped
      .map((name, min, max) => {
        JSONObject()
          .setAttr("name", name)
          .setAttr("min", min)
          .setAttr("max", max)
      })

    JSONArray(objects).toString
  }
}