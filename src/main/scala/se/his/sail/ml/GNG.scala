package se.his.sail.ml
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.ml.linalg.{Vector => SparkVector}
import org.apache.spark.rdd.RDD
import breeze.{linalg => br}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


class GNG private (
                    var iterations: Int, // maximum iterations
                    var lambda: Int, // how often to add new nodes/units
                    var maxNodes: Int, // maximum number of nodes
                    var eps_b: Double, // adaptation step size for closest node
                    var eps_n: Double, // adaptation step size for neighbors
                    var maxAge: Int, // maximum edge age
                    var alpha: Double, // error reduction rate for the neighbors of a newly created node
                    var d: Double, // error reduction rate for all nodes
                    var k: Double,
                    var moving: Boolean, // whether it should model a moving distribution
                    var untangle: Boolean, // constraints the creation of edges
                    var sampleSignals: Double, // fraction of the total amount of signals to be sampled during training.
                    var minNetSize: Int // minimum size (i.e., number of units) of a network.
                     ) {

  def this() = this(50, 100, 100, .2, .006, 50, .5, .995, 0, false, true, 1, 3)

  var inputCol = "features"

  def setInputCol(col: String): this.type = {
    this.inputCol = col
    this
  }

  def setIterations(iterations: Int): this.type = {
    this.iterations = iterations
    this
  }

  def setLambda(lambda: Int): this.type = {
    this.lambda = lambda
    this
  }

  def setMaxNodes(maxNodes: Int): this.type = {
    this.maxNodes = maxNodes
    this
  }

  def setMaxAge(maxAge: Int): this.type = {
    this.maxAge = maxAge
    this
  }

  def setEpsB(eps: Double): this.type = {
    this.eps_b = eps
    this
  }

  def setEpsN(eps: Double): this.type = {
    this.eps_n = eps
    this
  }

  def setAlpha(alpha: Double): this.type = {
    this.alpha = alpha
    this
  }

  def setD(d: Double): this.type = {
    this.d = d
    this
  }

  def setK(k: Double): this.type = {
    this.k = k
    this
  }

  def setMoving(moving: Boolean): this.type = {
    this.moving = moving
    this
  }

  def setUntangle(disentangle: Boolean): this.type = {
    this.untangle = disentangle
    this
  }

  def setSampleSignals(fraction: Double): this.type = {
    this.sampleSignals = fraction
    this
  }

  def setMinNetSize(minNetSize: Int): this.type = {
    this.minNetSize = minNetSize
    this
  }

  def getIterations: Int = this.iterations
  def getLambda: Int = this.lambda
  def getMaxNodes: Int = this.maxNodes
  def getEpsB: Double = this.eps_b
  def getEpsN: Double = this.eps_n
  def getMaxAge: Int = this.maxAge
  def getAlpha: Double = this.alpha
  def getD: Double = this.d
  def getK: Double = this.k
  def isMoving: Boolean = this.moving
  def isUntangle: Boolean  = this.untangle
  def getInputCol: String = this.inputCol
  def getSampleSignals: Double = this.sampleSignals
  def getMinNetSize: Int = this.minNetSize


  def fit(ds: Dataset[_]): GNGModel = {
    val rdd = ds
      .select(this.inputCol)
      .rdd.map{
        case Row(f: SparkVector) => new br.DenseVector(f.toArray)
      }

    rdd.persist()
    val model = this.fit(rdd)
    rdd.unpersist()

    model
  }

  def fit(rdd: RDD[br.DenseVector[Double]]): GNGModel = this.fit(rdd, GNGModel(rdd))

  def fit(rdd: RDD[br.DenseVector[Double]], model: GNGModel): GNGModel = {
    val mapFit = GNG.fit(
      lambda,
      maxNodes,
      eps_b, eps_n,
      maxAge,
      alpha, d, k,
      moving, untangle, sampleSignals, minNetSize) _

    val reduceFit = GNG.fit(
      lambda,
      maxNodes,
      eps_b, eps_n,
      maxAge,
      alpha, d, k,
      moving, untangle, 1, minNetSize) _

    var finalModel = model

    // Training...
    for (_ <- 0 until this.iterations) {
      finalModel = rdd
        .mapPartitions[GNGModel]((signals: Iterator[br.DenseVector[Double]]) => {
          val signalsArr = signals.toArray
          val partitionModel = mapFit(finalModel, signalsArr)
          Seq(partitionModel).iterator
        })
        .reduce{ (acc: GNGModel, m: GNGModel) =>
          reduceFit(acc, m.nodes.map(_.prototype))
        }
    }

    finalModel.setInputCol(this.inputCol)
  }

  def fitSequential(signals: Seq[br.DenseVector[Double]]): GNGModel = fitSequential(signals, GNGModel(signals))

  def fitSequential(signals: Seq[br.DenseVector[Double]], model: GNGModel): GNGModel = {
    val optimize = GNG.fit(
      lambda,
      maxNodes,
      eps_b, eps_n,
      maxAge,
      alpha, d, k,
      moving, untangle, sampleSignals, minNetSize) _

    var finalModel = model

    for (_ <- 0 until iterations) {
      finalModel = optimize(finalModel, signals)
    }

    finalModel.setInputCol(this.inputCol)
  }
}

case object GNG {
  def fit(lambda: Int,
          maxNodes: Int,
          eps_b: Double,
          eps_n: Double,
          maxAge: Int,
          alpha: Double,
          d: Double,
          k: Double,
          moving: Boolean,
          untangle: Boolean,
          sampleSignals: Double,
          minNetSize: Int
         )(model: GNGModel, inputSignals: Seq[br.DenseVector[Double]]): GNGModel = {

    var units = model.nodes
    var edges = model.edges
    val totalSignals = inputSignals.size
    val signalIterator = inputSignals.iterator
    var signalCounter = 0
    val maxSignals: Int = if (sampleSignals >= 1) 0 else (sampleSignals * totalSignals).toInt

    while((maxSignals <= 0 && signalIterator.hasNext) || (maxSignals > 0 && signalCounter < maxSignals)) {
      val signal = if (maxSignals > 0) {
        inputSignals(Random.nextInt(totalSignals))
      } else {
        signalIterator.next()
      }
      signalCounter += 1

    /**
        * 2. Find the nearest unit S1 and the second-nearest unit S2.
        **/
      var unitA, unitB: Node = null
      var distA, distB: Double = Double.MaxValue

      for (u <- units) {
        val d = u.distanceTo(signal)
        if (d < distA) {
          if (distA < distB) {
            distB = distA
            unitB = unitA
          }
          distA = d
          unitA = u
        } else if (d < distB) {
          distB = d
          unitB = u
        }
      }

      /**
        * 4. Add the squared distance between the input signal and
        * the nearest unit in input space to a local counter variable.
        **/
      unitA.error += distA * distA
      unitA.utility += (distB * distB) - (distA * distA)
      unitA.winCounter += 1

      /**
        * 3. Increment the age of all edges emanating from S1.
        *
        * 5. Move S1 and its direct topological neighbors towards E by
        * fractions Eb and En, respectively, of the total distance.
        **/
      unitA.moveTowards(signal, eps_b)

      var abEdge: Option[Edge] = None
      edges.foreach(e => {
        if (e.connects(unitA)) {
          e.age += 1
          e.getPartnerOf(unitA).moveTowards(signal, eps_n)

          if (e.connects(unitB)) abEdge = Some(e)
        }
      })

      /**
        * 6. If S1 and S2 are connected by an edge, set the age of this
        * edge to zero. If such an edge does not exist, create it.
        **/
      abEdge match {
        case Some(e) =>
          e.age = 0

        case None =>
          if (!untangle || (isTwoDimensional(unitA, unitB, edges) || networkSizeCompare(unitB, edges, minNetSize))) {
            edges.append(new Edge(unitA, unitB))
          }
      }

      /**
        * 7. Remove edges with an age larger than maxAge. If this results in
        * points having no emanating edges, remove them as well.
        **/
      edges = edges.filter(_.age <= maxAge)
      units = units.filter(n => edges.exists(_.connects(n)))

      /**
        * 9. Decrease all error variables by multiplying them with a constant d.
        **/
      units.foreach(n => {
        n.error *= d
        n.utility *= d
      })

      /**
        * 8. If the number of input signals generated so far is an integer
        * multiple of a parameter A, insert a new unit as follows.
        **/
      if (signalCounter % lambda == 0) {
        /**
          * Determine the unit q with the maximum accumulated error.
          **/
        val q = units.maxBy(_.error)

        /**
          * Remove obsolete nodes (in case of moving distribution).
          * */
        if (moving && units.lengthCompare(maxNodes) >= 0) {
          val i = units
            .filter(n => n.id != q.id && n.utility > 0)
            .minBy(_.utility)

          if (q.error / i.utility > k) {
            units = units.filter(_.id != i.id)
            edges = edges.filter(!_.connects(i))
          }
        }

        if (units.lengthCompare(maxNodes) < 0) {
          /**
            * Insert a new unit r halfway between q and its neighbor f with
            * the largest error variable
            **/
          val f = edges
            .filter(_.connects(q))
            .maxBy(_.getPartnerOf(q).error)
            .getPartnerOf(q)

          val newVector = (q.prototype + f.prototype) * .5
          val r = model.createUnit(newVector)
          units.append(r)

          /**
            * Insert edges connecting the new unit r with units q and f,
            * and remove the original edge between q and f.
            **/
          edges = edges.filterNot(_.connects(q, f))
          edges.append(new Edge(q, r))
          edges.append(new Edge(f, r))

          /**
            * Decrease the error variables of q and f by multiplying them
            * with a constant alpha. Initialize the error variable of r with
            * the new value of the error variable of q.
            **/
          q.error = q.error * alpha
          f.error = f.error * alpha
          r.error = q.error
        }

      }
    }

    model.nodes = units
    model.edges = edges
    model
  }

  def areCloseNeighbors(a: Node, b: Node, edges: ArrayBuffer[Edge], maxSteps: Int = 2): Boolean = {
    var openEdges = edges
    var openNodes = ArrayBuffer(a)
    var neighborsCount = 0
    var steps = 1

    while (neighborsCount <= 1 && steps <= maxSteps && openEdges.nonEmpty) {
      var nextUnits: ArrayBuffer[Node] = ArrayBuffer.empty

      for (u <- openNodes if neighborsCount <= 1) {
        neighborsCount += openEdges.count(_.connects(u, b))
        if (neighborsCount <= 1) {
          nextUnits = openEdges.filter(_.connects(u)).map(_.getPartnerOf(u))
          openEdges = openEdges.filterNot(_.connects(u))
        }
      }
      openNodes = nextUnits
      steps += 1
    }

    neighborsCount == 1
  }

  def isTwoDimensional(a: Node, b: Node, edges: ArrayBuffer[Edge]): Boolean = {
    def bridgesBetween(x: Node, y: Node, links: ArrayBuffer[Edge]): List[Node] = {
      val xNeighbors: mutable.Set[Node] = mutable.Set.empty
      val yNeighbors: mutable.Set[Node] = mutable.Set.empty

      for (e <- links) {
        if (e.connects(x)) xNeighbors.add(e.getPartnerOf(x))
        else if (e.connects(y)) yNeighbors.add(e.getPartnerOf(y))
      }

      xNeighbors.intersect(yNeighbors).toList
    }

    val bridges = bridgesBetween(a, b, edges)

    bridges match {
      // one common neighbor
      case bn :: Nil =>
        val closeEdges = edges.filter(e => e.connects(bn) || e.connects(a) || e.connects(b) )
        bridgesBetween(bn, b, closeEdges).length < 2 && bridgesBetween(bn, a, closeEdges).length < 2

      // two common neighbors
      case bn1 :: bn2 :: Nil =>
        !edges.exists(_.connects(bn1, bn2))

      case _ => false
    }
  }

  def existsPath(a: Node, b: Node, edges: ArrayBuffer[Edge]): Boolean = {
    val openNodes: mutable.Queue[Node] = mutable.Queue(a)
    var openEdges: ArrayBuffer[Edge] = edges
    var exists = false

    while (openEdges.nonEmpty && openNodes.nonEmpty && !exists) {
      exists = openEdges.exists(_.connects(openNodes.front, b))

      if (!exists) {
        val nextEdges: ArrayBuffer[Edge] = ArrayBuffer.empty
        val iterator = openEdges.iterator
        val head = openNodes.dequeue()

        while (iterator.hasNext && !exists) {
          val e = iterator.next()
          if (e.connects(head)) {
            openNodes.enqueue(e.getPartnerOf(head))
            exists = e.connects(b)
          } else {
            nextEdges.append(e)
          }
        }

        openEdges = nextEdges
      }
    }

    exists
  }

  def networkSizeCompare(a: Node, edges: ArrayBuffer[Edge], n: Int): Boolean = {
    val openNodes: mutable.Queue[Node] = mutable.Queue(a)
    var openEdges: ArrayBuffer[Edge] = edges
    var count = 1

    while (openEdges.nonEmpty && openNodes.nonEmpty && count <= n) {
      val nextEdges: ArrayBuffer[Edge] = ArrayBuffer.empty
      val iterator = openEdges.iterator
      val head = openNodes.dequeue()

      while (iterator.hasNext && count <= n) {
        val e = iterator.next()
        if (e.connects(head)) {
          openNodes.enqueue(e.getPartnerOf(head))
          count += 1
        } else {
          nextEdges.append(e)
        }
      }
      
      openEdges = nextEdges
    }

    count <= n
  }

}
