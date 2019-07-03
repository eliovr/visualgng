package se.his.sail.ml

import breeze.linalg.DenseVector

/** General representation of a data point.
  *
  * @param features A dense vector of doubles (see Breeze Scala library).
  * @param label An optional integer value representing the class or label.
  * */
case class Instance(features: DenseVector[Double], label: Option[Int])
