package se.his.sail.ml

import breeze.linalg.DenseVector
import org.apache.spark.ml.linalg.{Vector => SparkVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import se.his.sail.Instance


class GNG private (
  private var maxIterations: Int,   // maximum iterations
  private var lambda: Int,    // how often to add new nodes/units
  private var maxNodes: Int,  // maximum number of nodes
  private var eps_b: Double,  // adaptation step size for closest node
  private var eps_n: Double,  // adaptation step size for neighbors
  private var maxAge: Int,    // maximum edge age
  private var alpha: Double,  // error reduction rate for the neighbors of a newly created node
  private var d: Double,      // error reduction rate for all nodes
  private var k: Double) {

  def this() = this(15000, 100, 100, .2, .006, 50, .5, .995, 3)

  private var featuresCol = "features"

  private var labelCol: Option[String] = None

  /**
    * Maximum number of iterations.
    * */
  def getMaxIterations: Int = this.maxIterations

  /**
    * Set maximum number of iterations
    * */
  def setMaxIterations(i: Int): this.type = {
    require(i > 0)
    this.maxIterations = i
    this
  }

  /**
    * Frequency with which new nodes are added i.e. maxIterations % lambda == 0
    * */
  def getLambda: Int = this.lambda

  /**
    * Set frequency with which new nodes are added i.e. maxIterations % lambda == 0
    * */
  def setLambda(l: Int): this.type = {
    require(l > 0)
    this.lambda = l
    this
  }

  def getMaxNodes: Int = this.maxNodes

  def setMaxNodes(n: Int): this.type = {
    this.maxNodes = n
    this
  }

  def getEpsB: Double = this.eps_b

  def setEpsB(e: Double): this.type = {
    this.eps_b = e
    this
  }

  def getEpsN: Double = this.eps_n

  def setEpsN(e: Double): this.type = {
    this.eps_n = e
    this
  }

  def getMaxAge: Int = this.maxAge

  def setMaxAge(a: Int): this.type = {
    this.maxAge = a
    this
  }

  def getAlpha: Double = this.alpha

  def setAlpha(a: Double): this.type = {
    this.alpha = a
    this
  }

  def getD: Double = this.d

  def setD(d: Double): this.type = {
    this.d = d
    this
  }

  def getK: Double = this.k

  def setK(k: Double): this.type = {
    this.k = k
    this
  }

  def getFeaturesCol: String = this.featuresCol

  def setFeaturesCol(col: String): this.type = {
    this.featuresCol = col
    this
  }

  def setLabelCol(col: String): this.type = {
    this.labelCol = Some(col)
    this
  }

  // ------------ Optimization parameters / controls ---------

  def fit(df: Dataset[_], iterations: Int): GNGModel = {
    val rdd = df
      .select(featuresCol)
      .rdd
      .map{ case Row(f: SparkVector) => Instance(new DenseVector(f.toArray), None) }

    /**
      * 0. Start with two units a and b at random positions Wa and Wb in Rn.
      * */

    val Array(s1, s2) = rdd.takeSample(false, 2)

    val a = new Node(1, s1.features)
    val b = new Node(2, s2.features)

    var model  = new GNGModel(a :: b :: Nil, new Edge(a, b) :: Nil)

    rdd.persist()
    model = this.fit(rdd, model, iterations)
    rdd.unpersist()
    model

  }

  def fit(df: Dataset[_]): GNGModel = this.fit(df, this.maxIterations)

  def fit(rdd: RDD[Instance], model: GNGModel, iterations: Int): GNGModel = {
    var signalIterator: Iterator[Instance] = Iterator.empty
    var nodes = model.getNodes
    var edges = model.getEdges
    var iterationCounter = 0

    while (iterationCounter < iterations) {
      if (signalIterator.isEmpty || !signalIterator.hasNext)
        signalIterator = rdd
          .takeSample(withReplacement = true, this.lambda)
          .iterator

      /**
        * 1. Generate an input signal E according to P(E).
        * */
      while (signalIterator.hasNext && iterationCounter < iterations) {
        iterationCounter += 1
        val inputSignal = signalIterator.next()
        val vector = inputSignal.features

        /**
          * 2. Find the nearest unit S1 and the second-nearest unit S2.
          * */
        val distances = nodes
          .map(n => (n, n.distanceTo(vector)))
          .sortBy(_._2)

        val (node1, dist1) = distances.head
        val (node2, dist2) = distances.tail.head

        inputSignal.label match {
          case Some(l) =>
            node1.setLabel(l.toInt)
            node2.setLabel(l.toInt)
          case None =>
        }

        inputSignal.label match {
          case Some(l) =>
            node1.setLabel(l.toInt)
            node2.setLabel(l.toInt)
          case None =>
        }

        /**
          * 4. Add the squared distance between the input signal and
          * the nearest unit in input space to a local counter variable.
          * */
        node1.error += dist1 * dist1
        node1.utility += (dist2 * dist2) - (dist1 * dist1)
        node1.winCounter += 1

        /**
          * 3. Increment the age of all edges emanating from S1.
          *
          * 5. Move S1 and its direct topological neighbors towards E by
          * fractions Eb and En, respectively, of the total distance.
          * */
        node1.moveTowards(vector, eps_b)

        edges.foreach{ e =>
          if (e.has(node1)) {
            e.age += 1
            e.getPartner(node1).moveTowards(vector, eps_n)
          }
        }

        /**
          * 6. If S1 and S2 are connected by an edge, set the age of this
          * edge to zero. If such an edge does not exist, create it.
          * */
        edges.find(e => e.has(node1) && e.has(node2)) match {
          case Some(e) => e.age = 0
          case None => edges = new Edge(node1, node2) :: edges
        }

        /**
          * 7. Remove edges with an age larger than maxAge. If this results in
          * points having no emanating edges, remove them as well.
          * */
        edges = edges.filter(_.age <= maxAge)
        nodes = nodes.filter(n => edges.exists(_.has(n)))


        /**
          * 8. If the number of input signals generated so far is an integer
          * multiple of a parameter A, insert a new unit as follows.
          * */
        if (iterationCounter % lambda == 0) {
          var nodeCount = nodes.size

          /**
            * Determine the unit q with the maximum accumulated error.
            * */
          val q = nodes.maxBy(_.error)

          /**
            * Remove obsolete nodes.
            * */
          if (nodeCount >= this.maxNodes) {
            val i = nodes
              .filter(n => n.id != q.id && n.utility > 0)
              .minBy(_.utility)

            if (q.error / i.utility > k) {
              nodes = nodes.filter(_.id != i.id)
              edges = edges.filter(!_.has(i))
              nodeCount -= 1
            }
          }

          if (nodeCount < this.maxNodes) {
            /**
              * Insert a new unit r halfway between q and its neighbor f with
              * the largest error variable
              * */
            val f = edges
              .filter(_.has(q))
              .maxBy(_.getPartner(q).error)
              .getPartner(q)

            val newVector = (q.prototype + f.prototype) * .5
            val r = model.createNode(newVector)
            nodes = r :: nodes

            /**
              * Insert edges connecting the new unit r with units q and f,
              * and remove the original edge between q and f.
              * */
            edges = new Edge(q, r) :: new Edge(f, r) :: edges.filter(e => !(e.has(q) && e.has(f)))

            /**
              * Decrease the error variables of q and f by multiplying them
              * with a constant alpha. Initialize the error variable of r with
              * the new value of the error variable of q.
              * */
            q.error = q.error * alpha
            f.error = f.error * alpha
            r.error = q.error
          }
        }

        /**
          * 9. Decrease all error variables by multiplying them with a constant d.
          * */
        nodes.foreach(n => {
          n.error *= d
          n.utility *= d
        })
      }
    }

    new GNGModel(nodes, edges)
      .setNodeId(model.getNodeId)
      .setFeaturesCol(this.getFeaturesCol)
  }

}
