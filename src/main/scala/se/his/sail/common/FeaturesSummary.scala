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

//case class Stats(count: Long = 1,
//                 sum: Double = 0,
//                 min: Double = Double.MaxValue,
//                 max: Double = Double.MinValue) extends Serializable {
//
//   def +(other: Stats): Stats = other match {
//     case Stats(_, s, mn, mx) => Stats(count + 1, sum + s, mn.min(min), mx.max(max))
//   }
//
//   def +(x: Double): Stats = Stats(count + 1, sum + x, x.min(min), x.max(max))
//
//   def avg: Double = sum / count
//}
