package se.his.sail.zeppelin

import breeze.{linalg => br}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors, Vector => SparkVector}
import org.apache.spark.ml.stat.Summarizer
//import org.apache.spark.mllib
//import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.zeppelin.display.angular.notebookscope.AngularElem._
import org.slf4j.LoggerFactory
import se.his.sail.ml._
import se.his.sail.common.{JSONArray, JSONObject, Utils, FeaturesSummary}

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
class VisualGNG private (val id: Int, private var df: DataFrame) {

  private val logger = LoggerFactory.getLogger(classOf[VisualGNG])

  private val spark = df.sparkSession

  import spark.implicits._

  private val allowedDataTypes = List(DoubleType, IntegerType, BooleanType, FloatType, SQLDataTypes.VectorType)

  private var rdd: RDD[br.DenseVector[Double]] = spark.sparkContext.emptyRDD

  private val gng = new GNG().setIterations(1)

  var model: GNGModel = _

  private val dataHub = DataHub()
  private val pc = ParallelCoordinates(dataHub)
  private val fdg = ForceDirectedGraph(dataHub)

  def getSelected: String = this.dataHub.get


  // ------------- process variables -------------

  /**
    * Whether to continue to iterate (to fit the model to the data) or not.
    * */
  private var isTraining = false
  private var maxEpochs = 100
  private var epochs = 0
  private var accTime = .0

  /**
    * How often (training iterations) to update the Force Directed Graph.
    * */
  var updateEvery = 100


  // ------------- training parameters -------------

  private var inputCols: Option[Array[String]] = None

  private var labelCol: Option[String] = None

  private var idCol: Option[String] = None

  private var scaleValues =  false

  private var isImages = false

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

  def setIsImages(isImages: Boolean): this.type = {
    this.isImages = isImages
    this
  }


  /**
    * Names of the features used in training. This may vary depending on the previous three.
    * */
  private var features: FeaturesSummary = _

  /**
    * DataFrame column where the feature vectors are found.
    * */
  private var inputCol: String = "gng_features"


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
//  private var featureSelect: Select = _


  private val maxEpochsInput: InputNumber = new InputNumber(this.maxEpochs)
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

  private val untangleInput: Checkbox = new Checkbox(this.gng.isUntangle)

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
         |${this.maxEpochsInput.jsGetElementById}.disabled = false;
         |${this.maxNodesInput.jsGetElementById}.disabled = false;
         """.stripMargin)
    .setOnClickListener(() => {
      this.executionButton.set("Run")
      initResetTraining()
    })

  this.applyButton.setOnClickListener(() => {
    this.maxEpochs = this.maxEpochsInput.get.toInt
    this.gng.setMaxNodes(this.maxNodesInput.get.toInt)
    this.gng.setMaxAge(this.maxAgeInput.get.toInt)
    this.gng.setLambda(this.lambdaInput.get.toInt)
    this.gng.setEpsB(this.epsBInput.get)
    this.gng.setEpsN(this.epsNInput.get)
    this.gng.setAlpha(this.alphaInput.get)
    this.gng.setD(this.dInput.get)
    this.gng.setUntangle(this.untangleInput.get)
  })

  private def initialize(): Unit = {
    logger.info("Initializing input data (extracting features and statistics)")


    try {
      var outputCol = this.inputCol
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

      val featureNames = inputCols.zipWithIndex.flatMap{case (col, i) =>
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

      this.df = tempDF
      this.inputCol = outputCol
      this.rdd = df.select(this.inputCol).rdd.map{
        case Row(f: SparkVector) => new br.DenseVector(f.toArray)
      }

      logger.info("Computing feature stats.")
      val (min, max): (SparkVector, SparkVector) = df.select(
        Summarizer.metrics("min", "max")
          .summary(df(this.inputCol)).as("summary"))
        .select("summary.min", "summary.max")
        .as[(SparkVector, SparkVector)]
        .first()

      this.features = FeaturesSummary(featureNames, min.toArray, max.toArray)
      this.fdg.setFeatures(this.features)
      this.pc.setFeatures(this.features)
    }
    catch {
      case e: Throwable =>
        logger.error("VisualGNG ERROR: ", e)
        new Alert(e.getMessage).elem.display()
    }

    this.gng.setInputCol(this.inputCol)
  }

  private def initResetTraining(): Unit = {
    logger.info("Resetting training settings.")

    this.epochs = 0
    this.accTime = .0
    this.model = GNGModel(this.rdd)

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
            <li class="dropdown-header">Max epochs
              <span class="glyphicon glyphicon-info-sign" title="Maximum number of epochs (passes over the whole dataset)"></span>
            </li>
            <li onclick="event.stopPropagation();">{ maxEpochsInput.elem }</li>

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

            <li class="dropdown-header">Untangle
              <span class="glyphicon glyphicon-info-sign" title="Constrain the creation of edges"></span>
            </li>
            <li onclick="event.stopPropagation();">{ untangleInput.elem }</li>

            <li class="divider"></li>
            <li class="text-center">{ applyButton.elem }</li>
          </ul>
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
    val fitFunc = gng.fit(rdd) _

    try {
      while (this.isTraining) {
        /** Sampler (S). */
//        val sample = rdd.takeSample(withReplacement = true, gng.getLambda, this.iterationCounter)

        accTime += Utils.performance {
          /** Optimizer (O). */
          this.model = fitFunc(this.model)
        }

        epochs += 1

        /** Report learning state (El). */
        updateStats()

        /** Visualization Transformation (U). */
        updateGraph()

        /** Wait at least .1 seconds. Otherwise updates are too fast for user involvement. */
        val meanTime = accTime / epochs.toDouble
        if (meanTime < .1) {
          Thread.sleep(((.1 - meanTime) * 1000).toLong)
        }

        if (this.epochs >= this.maxEpochs) {
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
    val meanTime = accTime / epochs.toDouble
    var str = "Epochs: " + "%03d".format(this.epochs) + "/" + maxEpochs
    str += f" ($meanTime%1.2f s/epoch)"
    str += " | Nodes: " + "%03d".format(this.model.nodes.size)
    str += " | Edges: " + "%04d".format(this.model.edges.size)
    this.statusText.set(str)
  }


  private def updateGraph(): Unit = {
    val nodes: Iterable[JSONObject] = this.model.nodes.map{n =>
      val obj = JSONObject()
        .setAttr("id", n.id)
        .setAttr("density", n.winCounter)
        .setAttr("data", JSONArray(n.prototype.toArray))

      n.label match {
        case Some(x) => obj.setAttr("hint", x)
        case None => obj
      }
    }

    val edges: Iterable[JSONObject] = this.model.edges.map{e =>
      JSONObject()
        .setAttr("source", this.model.nodes.indexWhere(_.id == e.source.id))
        .setAttr("target", this.model.nodes.indexWhere(_.id == e.target.id))
        .setAttr("distance", e.distance)
    }

    val data: JSONObject = JSONObject()
      .setAttr("nodes", JSONArray(nodes))
      .setAttr("links", JSONArray(edges))

    dataHub.put(data.toString)
  }


  /**
    * Display parallel coordinates.
    * */
  def parallelCoordinates(): Unit = pc.display


  /**
    * Returns a DataFrame with each data point assigned to its closest unit.
    * */
  def transform(updateGraph: Boolean = false): Dataset[_] = {
    val predictions = this.model.transform(df)

    if (updateGraph) {
      import org.apache.spark.sql.functions._
      import spark.implicits._

      val stats: Array[(Int, Long, String)] = this.labelCol match {
        case Some(label) =>
          predictions
            .groupBy(model.outputCol)
            .agg(collect_list(label))
            .map{
              case Row(unitId: Int, labels: Seq[_]) =>
                val label = labels
                  .groupBy(identity)
                  .maxBy(_._2.size)._1
                (unitId, labels.size.toLong, label.toString)
            }
            .collect()
        case None =>
          predictions
            .groupBy(model.getOutputCol)
            .count()
            .map(row => (row.getInt(0), row.getLong(1), ""))
            .collect()
      }

      this.model.nodes.zipWithIndex.foreach{ case (n, i) =>
        stats.find(_._1 == i) match {
          case Some((_, count, label)) =>
            n.winCounter = count
            if (label.nonEmpty)
              n.setLabel(label)
          case None =>
            n.winCounter = 0
        }
      }

      this.updateGraph()
    }

    predictions
  }


  /**
    * Compute K-Means on the vectors held by the nodes and returns a fitted model.
    * It also updates the nodes in the graph by associating each to a cluster.
    *
    * @param k Number of clusters / centroids.
    * @return K-Means model fitted to the graph nodes.
    * */
  def kmeans(k: Int): KMeansModel = {
    val nodes = this.model.nodes
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
          case Some((n, _)) => n.setLabel(cluster.toString)
          case None =>
        }
      })

    updateGraph()
    kmModel
  }

}

private case class NodeSchema(id: Int, centroid: SparkVector)

object VisualGNG {
  private var nextId = 0

  def apply(df: DataFrame): VisualGNG = {
    nextId += 1
    new VisualGNG(nextId, df)
  }
}