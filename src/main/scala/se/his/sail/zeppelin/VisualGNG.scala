package se.his.sail.zeppelin

import breeze.linalg.DenseVector
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors, Vector => SparkVector}
import org.apache.spark.mllib
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.zeppelin.display.angular.notebookscope.AngularElem._
import org.slf4j.LoggerFactory
import se.his.sail.ml._
import se.his.sail.{Stats, Utils}


/**
  * A Visual Analytics library for the Growning Neural Gas (GNG) algorithm (Fritzke)
  * for Apache Zeppelin.
  *
  * @param df Spark DataFrame with the data on which the GNG will be fitted.
  *           By default, all features (columns) in the DataFrame are used training the GNG.
  *           The setInputCols method allows to define which features to use for training.
  *           The setIdCol method allows to mark a feature as an ID, and will not be used in training GNG.
  *           The setLabelCol method allows to mark a feature as a label, and will not be used in training GNG.
  *           The setScale method sets the flag for scaling training features.
  * */
class VisualGNG (private var df: DataFrame) {

  private val logger = LoggerFactory.getLogger(classOf[GNG])

  private val spark = df.sparkSession

  import spark.implicits._

  private val allowedDataTypes =
    List(DoubleType, IntegerType, BooleanType, FloatType, SQLDataTypes.VectorType)


  private var rdd: RDD[Instance] = spark.sparkContext.emptyRDD

  private val gng = new GNG

  var model: GNGModel = new GNGModel()

  private val pc: ParallelCoordinates = new ParallelCoordinates()

  private val fdg = new ForceDirectedGraph


  // ------------- process variables -------------

  /**
    * Whether to continue to iterate (to fit the model to the data) or not.
    * */
  private var isTraining = false

  private var maxIterations = gng.getMaxIterations

  private var iterationCounter = 0

  /**
    * How often (training iterations) to update the Force Directed Graph.
    * */
  var updateEvery = 100


  // ------------- training parameters -------------

  private var inputCols: Option[Array[String]] = None

  private var labelCol: Option[String] = None

  private var idCol: Option[String] = None

  private var scaleValues =  false

  /**
    * Sets the features (DataFrame columns) to be used in GNG training (as used by VectorAssembler).
    * If not set, all features in the data set are used for training.
    *
    * @param cols An array of column names defining the features to be used in training.
    * */
  def setInputCols(cols: Array[String]): this.type = {
    this.inputCols = Some(cols)
    this
  }

  /**
    * Sets a feature (DataFrame column) to be treated as a label.
    * Treating a feature as a label tells VisualGNG to color the stroke of the nodes by label value.
    *
    * @param col Column name of the feature to be tagged as a label.
    * */
  def setLabelCol(col: String): this.type = {
    this.labelCol = Some(col)
    this
  }

  /**
    * Sets a feature (DataFrame column) to be treated as an ID.
    * Treating a feature as an ID tells VisualGNG to ignore that it during training, but to keep it in the DF.
    * It's an alternative to specifying all training features through InputCols but this one.
    *
    * @param col Column name of the feature to be tagged as an ID.
    * */
  def setIdCol(col: String): this.type = {
    this.idCol = Some(col)
    this
  }

  /**
    * Sets the scaling flag (default = false).
    * If true, Visual GNG will normalize all features to a unit standard deviation (uses StandardScaler).
    *
    * @param scale Whether to scale training features or not.
    * */
  def setScale(scale: Boolean): this.type = {
    this.scaleValues = scale
    this
  }

  /**
    * Names of the features used in training. This may vary depending on the previous three.
    * */
  private var featureNames: Array[String] = Array.empty

  private var indexedLabels: Array[String] = Array.empty

  /**
    * DataFrame column where the feature vectors are found.
    * */
  private var featuresCol: String = "gng_features"


  // ------------- User controls -------------

  /**
    * For starting, pausing or restarting training.
    * */
  private val executionButton: Button = new Button("Run", "btn btn-warning btn-sm")

  /**
    * For refreshing the model so that it can be trained again.
    * */
  private val refreshButton: Button = new  Button("Refresh")

  /**
    * For applying changes made on advanced parameters.
    */
  private val applyButton: Button = new Button("Apply", "btn btn-warning btn-sm")

  /**
    * Color by drop down select.
    * */
  private var featureSelect: Select = _


  private val maxIterInput: InputNumber = new InputNumber(this.maxIterations)
    .setMin(100)
    .setStep(10)

  private val maxNodesInput: InputNumber = new InputNumber(this.gng.getMaxNodes)
    .setMin(10)
    .setStep(10)

  private val lambdaInput: InputNumber = new InputNumber(this.gng.getLambda)
    .setMin(1)
    .setStep(10)

  private val maxAgeInput: InputNumber = new InputNumber(this.gng.getMaxAge)
    .setMin(1)
    .setStep(10)

  private val epsBInput: InputNumber = new InputNumber(this.gng.getEpsB)
    .setMin(.0)
    .setMax(1)
    .setStep(.1)

  private val epsNInput: InputNumber = new InputNumber(this.gng.getEpsN)
    .setMin(.0)
    .setMax(1)
    .setStep(.1)

  private val alphaInput: InputNumber = new InputNumber(this.gng.getAlpha)
    .setMin(.0)
    .setMax(1)
    .setStep(.1)

  private val dInput: InputNumber = new InputNumber(this.gng.getD)
    .setMin(.0)
    .setMax(1)
    .setStep(.1)


  /**
    * Text used for displaying the current status of the training.
    * i.e. iteration #, percentage, nodes and edges.
    * */
  private val statusText: Text = new Text("Iteration: 0").setAttribute("class", "text-muted")

  /**
    * Indicates the array index of the currently selected feature.
    * This value may change through _featureSelect.
    * */
  private var selectedFeature: Int = 0


  // ---------- Event listeners -------------
  // Note: Zeppelin-Angular onchange event listeners seem to be broken, so only onclick are used.

  this.executionButton
    .setOnClickListener(() => {
      this.isTraining = !this.isTraining

      if (this.isTraining) {
        executionButton.set("Pause")
        run()
      } else {
        this.executionButton.set("Run")
      }
    })

  this.refreshButton
    .setOnClickScript(
      s"""
         |${this.maxIterInput.jsGetElementById}.disabled = false;
         |${this.maxNodesInput.jsGetElementById}.disabled = false;
         """.stripMargin)
    .setOnClickListener(() => {
      this.executionButton.set("Run")
      initResetTraining()
    })

  this.applyButton.setOnClickListener(() => {
    this.maxIterations = this.maxIterInput.get.toInt
    this.gng.setMaxNodes(this.maxNodesInput.get.toInt)
    this.gng.setMaxAge(this.maxAgeInput.get.toInt)
    this.gng.setLambda(this.lambdaInput.get.toInt)
    this.gng.setEpsB(this.epsBInput.get)
    this.gng.setEpsN(this.epsNInput.get)
    this.gng.setAlpha(this.alphaInput.get)
    this.gng.setD(this.dInput.get)
  })

  this.fdg
    .setMouseoverScript(
      s"""
         |if (typeof ${pc.id} !== 'undefined') {
         |  let pc = ${pc.id};
         |  let lines = pc.getSeries();
         |
           |  lines
         |    .filter(function(d){ return !d.selected; })
         |    .attr('stroke-width', null)
         |    .attr('stroke-opacity', .25);
         |
           |  lines
         |    .filter(function(d){ return d.id === node.id; })
         |    .attr('stroke', pc.getStroke)
         |    .attr('stroke-width', 3)
         |    .attr('stroke-opacity', 1);
         |}
         """.stripMargin)
    .setMouseoutScript(
      s"""
         |let fdg = ${fdg.id};
         |
           |if (typeof ${pc.id} !== 'undefined') {
         |  let pc = ${pc.id};
         |  if (fdg.selectedCounter <= 0) {
         |    pc.getSeries()
         |      .attr('stroke', pc.getStroke)
         |      .attr('stroke-width', null)
         |      .attr('stroke-opacity', null);
         |  } else {
         |    pc.getSeries()
         |      .filter(function(d){ return !d.selected; })
         |      .attr('stroke-width', null)
         |      .attr('stroke-opacity', .25);
         |  }
         |}
         |
        """.stripMargin)
    .setClickedScript(
      s"""
         |if (typeof ${pc.id} !== 'undefined') {
         |  let pc = ${pc.id};
         |  let line = pc.getSeries().data()
         |    .find(function(d){ return d.id === node.id; });
         |
           |  if (line) line.selected = node.selected;
         |}
        """.stripMargin)
    .setOnUpdateScript(
      s"""
         |if (typeof ${pc.id} !== 'undefined')
         |  ${pc.scriptSetData("data")}
        """.stripMargin)

  this.pc
    .setOnDisplayScript(
      s"""
         |var data = ${fdg.id}.nodes.data();
         |if (typeof ${pc.id} !== 'undefined')
         |  ${pc.scriptSetData("data")}
      """.stripMargin)
    .setOnFilterScript(
      s"""
         |let fdg = ${fdg.id};
         |let nodes = fdg.nodes;
         |let selectedElems = elems.data();
         |let selectedNodes = nodes
         |  .filter(function(d) {
         |    for (let i = 0 ; i < selectedElems.length ; i++)
         |      if (selectedElems[i].id == d.id)
         |        return true;
         |
           |    return false;
         |  });
         |
           |nodes
         |  .attr('fill', null)
         |  .attr('stroke', null);
         |
           |selectedNodes
         |  .attr('fill', fdg.getFill)
         |  .attr('stroke', fdg.getStroke);
     """.stripMargin)


  private def initialize(): Unit = {
    try {
      var outputCol = this.featuresCol
      var tempDF: DataFrame = df.na.drop()

      var inputCols: Array[String] = this.inputCols match {
        case Some(cols) => cols
        case None =>
          val cols = tempDF.columns.map(_.replace(".", "_"))
          tempDF = tempDF.toDF(cols:_*)
          cols
      }

      val schemaFields = tempDF.schema.fields

      inputCols = inputCols.filter(col => {
        col != this.labelCol.orNull &&
          col != this.idCol.orNull &&
          (schemaFields.find(_.name == col) match {
            case Some(field) => this.allowedDataTypes.contains(field.dataType)
            case None => false
          })
      })

      val firstRow = tempDF.select(inputCols.head, inputCols.tail:_*).first()

      this.featureNames = inputCols.zipWithIndex.flatMap{case (col, i) =>
        firstRow.get(i) match {
          case _: Double => Array(col)
          case _: Int => Array(col)
          case _: Boolean => Array(col)
          case vec: SparkVector =>
            vec.toArray.indices.map(j => col + s"_$j")
        }
      }

      tempDF = new VectorAssembler()
        .setInputCols(inputCols)
        .setOutputCol(outputCol)
        .transform(tempDF)

      if (this.scaleValues) {
        val inputCol = outputCol
        outputCol = outputCol + "_scaled"

        tempDF = new StandardScaler()
          .setInputCol(inputCol)
          .setOutputCol(outputCol)
          .fit(tempDF).transform(tempDF)
      }

      if (this.labelCol.nonEmpty) {
        schemaFields.find(_.name == this.labelCol.get) match {
          case Some(sf) if sf.dataType == StringType =>
            this.labelCol = Some(sf.name + "_indexed")
            val siModel = new StringIndexer()
              .setInputCol(sf.name)
              .setOutputCol(this.labelCol.get)
              .fit(tempDF)

            tempDF = siModel.transform(tempDF)
            this.indexedLabels = siModel.labels
          case Some(sf) if sf.dataType == IntegerType || sf.dataType == DoubleType =>
          case _ =>
            this.labelCol = None
            new Alert("labelCol not found or unsupported data type").elem.display()
        }

      }

      this.df = tempDF
      this.featuresCol = outputCol
      this.rdd = this.labelCol match {
        case Some(col) => df
          .select(this.featuresCol, col)
          .rdd
          .map{
            case Row(f: SparkVector, l: Int) => Instance(new DenseVector(f.toArray), Some(l))
            case Row(f: SparkVector, l: Double) => Instance(new DenseVector(f.toArray), Some(l.toInt))
          }
        case None => df
          .select(this.featuresCol)
          .rdd
          .map{ case Row(f: SparkVector) => Instance(new DenseVector(f.toArray), None) }
      }
    }
    catch {
      case e: Throwable =>
        logger.error("VisualGNG ERROR: ", e)
        new Alert(e.getMessage).elem.display()
    }

    this.gng.setFeaturesCol(this.featuresCol)
    this.featureSelect = new Select(this.featureNames)
      .setOnClickListener(s => {
        val i = this.featureNames.indexWhere(_ == s)

        if (this.selectedFeature != i) {
          this.selectedFeature = i
          if (!this.isTraining) updateGraph()
        }
      })
  }

  private def initResetTraining(): Unit = {
    this.iterationCounter = 0
    this.model = this.gng.modelInitializer(this.rdd)

    updateGraph()
    updateStats()
  }


  /**
    * Show elements in notebook.
    * */
  def display: this.type = {
    initialize()

    logger.info("Displaying visual GNG")

    <div class="container">
      <div class="row">
        <div class="col col-lg-2 btn-group-btn" style="min-width: 200px">
          { executionButton.elem }
          { refreshButton.elem }
          <button type="button" class="btn btn-default dropdown-toggle btn-sm" data-toggle="dropdown">
            <span class="glyphicon glyphicon-cog"></span>
          </button>
          <ul class="dropdown-menu" style="padding: 5px" role="menu">
            <li class="dropdown-header">Max iterations
              <span class="glyphicon glyphicon-info-sign" title="Maximum number of iterations (input signals)"></span>
            </li>
            <li onclick="event.stopPropagation();">{ maxIterInput.elem }</li>

            <li class="dropdown-header">Max nodes
              <span class="glyphicon glyphicon-info-sign" title="Maximum number nodes"></span>
            </li>
            <li onclick="event.stopPropagation();">{ maxNodesInput.elem }</li>

            <li class="dropdown-header">Lamda
              <span class="glyphicon glyphicon-info-sign" title="Number of iterations to run before creating a new node"></span>
            </li>
            <li onclick="event.stopPropagation();">{ lambdaInput.elem }</li>

            <li class="dropdown-header">Max edge age
              <span class="glyphicon glyphicon-info-sign" title="How many iterations an 'obsolete' edge can live"></span>
            </li>
            <li onclick="event.stopPropagation();">{ maxAgeInput.elem }</li>

            <li class="dropdown-header">eps b
              <span class="glyphicon glyphicon-info-sign" title="Adaptation step size (closest node)"></span>
            </li>
            <li onclick="event.stopPropagation();">{ epsBInput.elem }</li>

            <li class="dropdown-header">eps n
              <span class="glyphicon glyphicon-info-sign" title="Adaptation step size (neighbors of closest node)"></span>
            </li>
            <li onclick="event.stopPropagation();">{ epsNInput.elem }</li>

            <li class="dropdown-header">alpha
              <span class="glyphicon glyphicon-info-sign" title="Error reduction rate for the neighbors of a newly created node"></span>
            </li>
            <li onclick="event.stopPropagation();">{ alphaInput.elem }</li>

            <li class="dropdown-header">d
              <span class="glyphicon glyphicon-info-sign" title="Error reduction rate for all nodes"></span>
            </li>
            <li onclick="event.stopPropagation();">{ dInput.elem }</li>

            <li class="divider"></li>
            <li class="text-center">{ applyButton.elem }</li>
          </ul>
        </div>

        <div class="col col-lg-3" style="min-width: 200px">
          <div class="input-group input-group-sm">
            <span class="input-group-addon" title="Color nodes based on the value of the selected feature">Color by</span>
            { featureSelect.elem }
          </div>
        </div>
      </div>

      <div class="row">
        <div class="col col-lg-12" style="overflow: auto;">
          { statusText.elem }
          { fdg.elem }
        </div>
      </div>
    </div>.display()

    initResetTraining()
    this
  }

  /**
    * Run the GNG algorithm while isTraining == true.
    * */
  private def run(): Unit = {
    this.rdd.persist()
    var t = .0
    var epoch = 0

    try {
      while (this.isTraining) {
        /** Sampler (S). */
        val sample = rdd.takeSample(withReplacement = true, num = gng.getLambda, seed = this.iterationCounter)

        t += Utils.performance {
          /** Optimizer (O). */
          this.model = gng.fit(sample, this.model)
        }

        // iterations are the number of seen signals.
        this.iterationCounter += gng.getLambda
        epoch += 1

        /** Report learning state (El). */
        updateStats()

        /** Visualization Transformation (U). */
        updateGraph()

        /** Wait at least .1 seconds. Otherwise updates are too fast for user involvement. */
        if (t / epoch < .1) Thread.sleep(((.1 - (t / epoch)) * 1000).toLong)

        if (this.iterationCounter >= this.maxIterations) {
          this.isTraining = false
          executionButton.set("Done")
        }
      }

    } catch {
      case e: Throwable =>
        statusText.set("This was... unexpected: " + e.getMessage)
        logger.error("VisualGNG ERROR: ", e)
    }
  }

  /**
    * Updates execution stats shown to the user.
    * */
  private def updateStats(): Unit = {
    val percentage = (this.iterationCounter / this.maxIterations.toDouble) * 100
    var str = "Iteration: " + "%05d".format(this.iterationCounter)
    str += " (%03d".format(percentage.toInt) + "%)"
    str += " | Nodes: " + "%03d".format(this.model.getNodes.size)
    str += " | Edges: " + "%04d".format(this.model.getEdges.size)
    this.statusText.set(str)
  }


  /**
    * Updates the force directed graph with the current state of the GNG model.
    * */
  private def updateGraph(
                           minRadius: Int = 5,
                           maxRadius: Int = 15,
                           maxEdgeDistance: Int = 50,
                           colorByFeature: Int = 0
                         ): Unit = {

    val nodes = this.model.getNodes
    val edges = this.model.getEdges

    val distanceStats = edges.foldLeft(Stats())(_ + _.distance())
    val counterStats = nodes.foldLeft(Stats())(_ + _.winCounter)
    val featureStats = nodes.foldLeft(Stats())(_ + _.prototype(this.selectedFeature))

    val nodeRadius: Node => Double = n =>
      Utils.scale(counterStats.min, counterStats.max)(0, maxRadius - minRadius)(n.winCounter) + minRadius

    val edgeDistance: Edge => Double = e =>
      Utils.scale(distanceStats.min , distanceStats.max)(0, maxEdgeDistance)(e.distance(false))

    val toHSL = Utils.spenceHSL(featureStats) _

    val graphNodes = nodes.map( n => {
      val radius = nodeRadius(n)
      val (hue, saturation, lightness) = toHSL(n.prototype(this.selectedFeature))
      val hint =  s"id: ${n.id} | count: ${Utils.toShortString(n.winCounter)}"

      val gn = new GraphNode(n.id)
        .setRadius(radius)
        .setHSL(hue.toInt, saturation, lightness)
        .setData(n.prototype.toArray.mkString("[", ",", "]"))

      n.label match {
        case Some(l) =>
          gn.setGroup(l)
          if (this.indexedLabels.isEmpty) gn.setHint(hint + s" | label: $l")
          else gn.setHint(hint + s" | label: ${this.indexedLabels(l)}")
        case None => gn.setHint(hint)
      }

      gn
    })

    val graphEdges = edges.map(e => {
      new GraphEdge(
        nodes.indexWhere(_.id == e.source.id),
        nodes.indexWhere(_.id == e.target.id),
        edgeDistance(e))
    })

    fdg.setData(graphNodes, graphEdges)
  }


  /**
    * Display parallel coordinates.
    * */
  def parallelCoordinates(): Unit = {
    if (!pc.isDisplayed) {
      val rddVectors = this.df
        .select(this.featuresCol)
        .rdd
        .map{ case Row(vec: SparkVector) => mllib.linalg.Vectors.fromML(vec) }

      pc
        .setFeatureNames(this.featureNames)
        .setStats(mllib.stat.Statistics.colStats(rddVectors))
    }

    pc.display()
  }


  /**
    * Returns a DataFrame with each data point assigned to its closest unit.
    *
    * @param withDistance Whether the distance from a data point and its closest unit
    *                     should be computed and added to the resulting DataFrame.
    * */
  def getPredictions(withDistance: Boolean = false): DataFrame =
    this.model.transform(df, withDistance = withDistance)


  /**
    * Compute K-Means on the vectors held by the nodes and returns a fitted model.
    * It also updates the nodes in the graph by associating each to a cluster.
    *
    * @param k Number of clusters / centroids.
    * @return K-Means model fitted to the graph nodes.
    * */
  def kmeans(k: Int): KMeansModel = {
    val nodes = this.model.getNodes
    val nodesInstances = nodes.zipWithIndex
      .map{ case (n, i) => NodeSchema(i, Vectors.dense(n.prototype.toArray)) }

    val nodesDS = spark.createDataset(nodesInstances)

    val kmModel = new KMeans()
      .setFeaturesCol("centroid")
      .setPredictionCol("prediction")
      .setK(k)
      .fit(nodesDS)

    kmModel
      .transform(nodesDS)
      .collect()
      .foreach(row => {
        val id = row.getAs[Int]("id")
        val cluster = row.getAs[Int]("prediction")

        nodes.zipWithIndex.find(_._2 == id) match {
          case Some((n, _)) => n.setLabel(cluster)
          case None =>
        }
      })

    updateGraph()
    kmModel
  }

  /**
    * Computes a full count of data points per unit and updates the graph.
    * */
  def computeDensity(): this.type = {
    val counts = getPredictions()
      .groupBy(model.getPredictionCol)
      .count()
      .collect()
      .map(row => (row.getInt(0), row.getLong(1)))

    this.model.getNodes.zipWithIndex.foreach{ case (n, i) =>
      counts.find(_._1 == i) match {
        case Some((_, count)) => n.winCounter = count
        case None => n.winCounter = 0
      }
    }

    updateGraph()
    this
  }

}

private case class NodeSchema(id: Int, centroid: SparkVector)
