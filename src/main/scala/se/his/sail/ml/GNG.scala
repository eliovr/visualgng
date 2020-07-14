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
                    var timeConstraint: Int,
                    var maxNeighbors: Int,
                    var maxSteps: Int
                     ) {

  def this() = this(15000, 100, 100, .2, .006, 50, .5, .995, 3, false, true, 0, 6, 2)

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

  def setTimeConstraint(seconds: Int): this.type = {
    this.timeConstraint = seconds
    this
  }

  def setMaxNeighbors(n: Int): this.type = {
    this.maxNeighbors = n
    this
  }

  def setMaxSteps(steps: Int): this.type = {
    this.maxSteps = steps
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
  def getMaxNeighbors: Int = this.maxNeighbors
  def getMaxSteps: Int = this.maxSteps


  def fit(ds: Dataset[_]): GNGModel = {
    val rdd = ds.select(this.inputCol).rdd.map{
      case Row(f: SparkVector) => new br.DenseVector(f.toArray)
    }.persist()

    val model = this.fit(rdd)
    rdd.unpersist()

    model
  }

  def fit(rdd: RDD[br.DenseVector[Double]]): GNGModel = this.fit(rdd, GNGModel(rdd, maxAge=maxAge))

  def fit(rdd: RDD[br.DenseVector[Double]], model: GNGModel): GNGModel = {
    val mapFit = GNG.fit(
      lambda,
      maxNodes,
      eps_b, eps_n,
      maxAge,
      alpha, d, k,
      moving, untangle, timeConstraint, maxNeighbors, maxSteps) _

    val reduceFit = GNG.fit(maxNodes*2,
      maxNodes,
      eps_b + eps_b, eps_n,
      maxAge,
      alpha, d, k,
      moving, untangle, 0, maxNeighbors, maxSteps) _

    val iterations = this.iterations

    val models: RDD[GNGModel] = rdd.mapPartitions[GNGModel]{ next: Iterator[br.DenseVector[Double]] =>
      val signals = next.toArray
      var _model = model

      for (_ <- 0 until iterations) {
        _model = mapFit(_model, signals)
      }

      Seq(_model).iterator
    }

    models.reduce{ (acc: GNGModel, m: GNGModel) =>
      reduceFit(acc, m.nodes.map(_.prototype))
    }.setInputCol(this.inputCol)
  }

  def fitSequential(arr: Array[br.DenseVector[Double]])(model: GNGModel = GNGModel(arr)): GNGModel = {
    val optimize = GNG.fit(
      lambda,
      maxNodes,
      eps_b, eps_n,
      maxAge,
      alpha, d, k,
      moving, untangle, timeConstraint, maxNeighbors, maxSteps) _

    var m = model
    for (_ <- 0 until iterations) {
      m = optimize(m, arr)
    }
    m
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
          timeConstraint: Int,
          maxNeighbors: Int,
          maxSteps: Int)(model: GNGModel, inputSignals: Seq[br.DenseVector[Double]]): GNGModel = {

    var units = model.nodes
    var edges = model.edges
    val totalSignals = inputSignals.size
    val signalIterator = inputSignals.iterator
    var accTime = .0
    var signalCounter = 0

    while((timeConstraint <= 0 && signalIterator.hasNext) || (timeConstraint > 0 && accTime < timeConstraint)) {
      val startTime = System.nanoTime()

      val signal = if (timeConstraint > 0) {
        inputSignals(Random.nextInt(totalSignals))
      } else {
        signalIterator.next()
      }
      signalCounter += 1

    /**
        * 2. Find the nearest unit S1 and the second-nearest unit S2.
        **/
      val distances = units
        .map(n => (n, n.distanceTo(signal)))
        .sortBy(_._2)

      val (unitA, distA): (Node, Double) = distances.head
      val (unitB, distB): (Node, Double) = distances.tail.head

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

      var countA, countB = 0  // use counts for disentanglement.
      var abEdge: Option[Edge] = None

      edges.foreach { e =>
        if (e.connects(unitA)) {
          e.age += 1
          e.getPartner(unitA).moveTowards(signal, eps_n)
          countA += 1
        } else if (e.connects(unitB)) {
          countB += 1
        }

        if (e.connects(unitA, unitB)) {
          abEdge = Some(e)
        }
      }

      /**
        * 6. If S1 and S2 are connected by an edge, set the age of this
        * edge to zero. If such an edge does not exist, create it.
        **/
      abEdge match {
        case Some(e) =>
          e.age = 0
          e.maxAge += 1 / maxAge

        case None =>
          if (!untangle || (countA <= maxNeighbors  && countB <= maxNeighbors && areCloseNeighbors(unitA, unitB, edges, maxSteps))) {
            edges.append(new Edge(unitA, unitB))
          }
      }

      /**
        * 7. Remove edges with an age larger than maxAge. If this results in
        * points having no emanating edges, remove them as well.
        **/
//      edges = edges.filter(_.age <= maxAge)
      edges = edges.filter(e => e.age <= e.maxAge)
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
            .maxBy(_.getPartner(q).error)
            .getPartner(q)

          val newVector = (q.prototype + f.prototype) * .5
          val r = model.createUnit(newVector)
          units.append(r)

          /**
            * Insert edges connecting the new unit r with units q and f,
            * and remove the original edge between q and f.
            **/
          edges = edges.filterNot(_.connects(q, f))
          edges.append(new Edge(q, r, maxAge=maxAge))
          edges.append(new Edge(f, r, maxAge=maxAge))

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

      accTime += (System.nanoTime() - startTime) / 1e9
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
          nextUnits = openEdges.filter(_.connects(u)).map(_.getPartner(u))
          openEdges = openEdges.filterNot(_.connects(u))
        }
      }
      openNodes = nextUnits
      steps += 1
    }

    neighborsCount == 1
  }

  def formsDimensionalObject(a: Node, b: Node, edges: ArrayBuffer[Edge], dimensionality: Int = 2): Boolean = {
    val neighborNodes = edges.filter(_.connects(a)).map(_.getPartner(a))
    val neighborsEdges = edges.filter(e => !e.connects(a) && neighborNodes.exists(e.connects) )
    val bridgeNodes = neighborsEdges.filter(_.connects(b)).map(_.getPartner(b))
    val bridgesCount = bridgeNodes.size
    var allowedDimensionality = bridgesCount > 0 && bridgesCount <= dimensionality

    if (bridgesCount == dimensionality) {
      var connections = 0
      for (i <- 0 until bridgesCount-1) {
        val n1 = bridgeNodes(i)
        for (j <- i+1 until bridgesCount) {
          val n2 = bridgeNodes(j)
          if (neighborsEdges.exists(_.connects(n1, n2))) {
            connections += 1
          }
        }
      }
      allowedDimensionality = connections < bridgesCount-1
    }
    allowedDimensionality
  }

  def existsPath(a: Node, b: Node, edges: ArrayBuffer[Edge]): Boolean = {
    var exists = false
    var openEdges = edges
    var openNodes = ArrayBuffer(a)
    val closedNodes: ArrayBuffer[Node] = ArrayBuffer.empty

    while (!exists && openEdges.nonEmpty) {
      var nextNodes: ArrayBuffer[Node] = ArrayBuffer.empty

      for (u <- openNodes if !exists) {
        exists = openEdges.exists(_.connects(u, b))
        if (!exists) {
          nextNodes = openEdges
            .filter(_.connects(u))
            .map(_.getPartner(u))
            .filterNot(closedNodes.contains)

          openEdges = openEdges.filterNot(_.connects(u))
        }
      }
      closedNodes ++= openNodes
      openNodes = nextNodes
    }

    exists
  }

  def networkCountCompare(a: Node, edges: ArrayBuffer[Edge], maxNodes: Int = 3): Boolean = {
    var openEdges = edges
    var openNodes = ArrayBuffer(a)
    val closedNodes: mutable.Set[Int] = mutable.Set(a.id)

    while (closedNodes.size <= maxNodes && openNodes.nonEmpty) {
      var nextNodes: ArrayBuffer[Node] = ArrayBuffer.empty

      for (u <- openNodes if closedNodes.size <= maxNodes) {
        nextNodes = openEdges.filter(e => e.connects(u) && !closedNodes.contains(e.getPartner(u).id)).map(_.getPartner(u))
        openEdges = openEdges.filterNot(e => e.connects(u))
        nextNodes.foreach(u => closedNodes.add(u.id))
      }
      openNodes = nextNodes
    }

    closedNodes.size <= maxNodes
  }

}