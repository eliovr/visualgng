package se.his.sail.ml
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.ml.linalg.{Vector => SparkVector}
import org.apache.spark.rdd.RDD
import breeze.{linalg => br}

import scala.collection.mutable.ArrayBuffer


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
                    var untangle: Boolean // constraints the creation of edges
                     ) {

  def this() = this(15000, 100, 100, .2, .006, 50, .5, .995, 3, false, true)

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


  def fit(ds: Dataset[_]): GNGModel = {
    val rdd = ds.select(this.inputCol).rdd.map{
      case Row(f: SparkVector) => new br.DenseVector(f.toArray)
    }.persist()
    val model = this.fit(rdd)()
    rdd.unpersist()
    model
  }

  def fit(rdd: RDD[br.DenseVector[Double]])(model: GNGModel = GNGModel.createModel(rdd)): GNGModel = {
    val mapFit = GNG.fit(lambda, maxNodes, eps_b, eps_n, maxAge, alpha, d, k, moving, untangle) _
    val reduceFit = GNG.fit(lambda, maxNodes, .5, eps_n, maxAge, alpha, d, k, moving, untangle) _
    val iterations = this.iterations

    val models: RDD[GNGModel] = rdd.mapPartitions[GNGModel]{ next: Iterator[br.DenseVector[Double]] =>
      val points = next.toArray
      var m = model

      for (_ <- 0 until iterations) {
        m = mapFit(m, points)
      }

      Seq(model).iterator
    }

    models.reduce{ (acc: GNGModel, m: GNGModel) =>
      reduceFit(acc, m.nodes.map(_.prototype))
    }.setInputCol(this.inputCol)
  }

  def fitSequential(arr: Array[br.DenseVector[Double]])(model: GNGModel = GNGModel.createModel(arr)): GNGModel = {
    val fitFunc = GNG.fit(lambda, maxNodes, eps_b, eps_n, maxAge, alpha, d, k, moving, untangle) _
    var m = model
    for (_ <- 0 until iterations) {
      m = fitFunc(m, arr)
    }
    m
  }
}

case object GNG {
  def fit(lambda: Int, maxNodes: Int, eps_b: Double, eps_n: Double, maxAge: Int, alpha: Double, d: Double, k: Double, moving: Boolean, disentangle: Boolean)
         (model: GNGModel, inputSignals: Seq[br.DenseVector[Double]]): GNGModel = {
    var units = model.nodes
    var edges = model.edges
    var signalCounter = 0

    for (signal <- inputSignals) {
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
        case Some(e) => e.age = 0

        case None =>  // -------------- Connect if 1 close neighbor and 4 connections at most
          if (!disentangle || (hasOneCloseNeighbor(unitA, unitB, edges) && countA <= 4  && countB <= 4))
            edges.append(new Edge(unitA, unitB))

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

  def hasOneCloseNeighbor(a: Node, b: Node, edges: ArrayBuffer[Edge], maxSteps: Int = 2): Boolean = {
    var _edges = edges
    var units = ArrayBuffer(a)
    var neighborsCount = 0
    var steps = 1
    while (neighborsCount <= 1 && steps <= maxSteps && _edges.nonEmpty) {
      var nextUnits: ArrayBuffer[Node] = ArrayBuffer.empty
      for (u <- units if neighborsCount <= 1) {
        neighborsCount += _edges.count(_.connects(u, b))
        if (neighborsCount <= 1) {
          nextUnits = _edges.filter(_.connects(u)).map(_.getPartner(u))
          _edges = _edges.filterNot(_.connects(u))
        }
      }
      units = nextUnits
      steps += 1
    }

    neighborsCount == 1
  }
}