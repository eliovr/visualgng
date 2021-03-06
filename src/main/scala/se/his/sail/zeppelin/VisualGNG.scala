package se.his.sail.zeppelin

import breeze.{linalg => br}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors, Vector => SparkVector}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.zeppelin.display.angular.notebookscope.AngularElem._
import org.slf4j.LoggerFactory
import se.his.sail.ml._
import se.his.sail.common.{FeaturesSummary, Utils}


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

  private val gng = new GNG()
    .setIterations(1)

  var model: GNGModel = _


  // ---------------- Frontend plots' interfaces ------------

  var maxDisplayFeatures: Int = 700
  /**
    * Names of the features used in training. This may vary depending on the previous three.
    * */
  private var features: FeaturesSummary = _

  private val dataHub = DataHub()

  private val pc = ParallelCoordinates(dataHub)

  private val fdg = ForceDirectedGraph(dataHub)

  // ------------- process variables -------------

  /**
    * Whether to continue to iterate (to fit the model to the data) or not.
    * */
  private var isTraining = false
  private var maxEpochs = 50
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

  private var sampleSize = 1.0

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

  def setImages(isImages: Boolean): this.type = {
    this.isImages = isImages
    this
  }

  /**
    * Sets the maximum number of features (data attributes) to display in the views (e.g., in the PC plot).
    * This is used in order to limit the burden on the views in the cases of high-dimensional data (e.g., images).
    * A summary of features is sent to the frontend, if there are too many features, then the browser can lag.
    * Default value 700.
    * */
  def setMaxDisplayFeatures(n: Int): this.type = {
    this.maxDisplayFeatures = n
    this
  }

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

  private val untangleInput: Checkbox = new Checkbox(this.gng.isUntangle)
    .setHint("Train using the untangle mechanism")

  private val maxEpochsInput: InputNumber = new InputNumber(this.maxEpochs)
    .setHint("Maximum number of epochs to run")
    .setMin(1)
    .setStep(5)

  private val maxSignalsInput: InputNumber = new InputNumber(this.sampleSize)
    .setHint("Fraction of the total amount of signals to be sampled during training (1 == all)")
    .setMin(.0)
    .setMax(1)
    .setStep(.1)

  private val maxNodesInput: InputNumber = new InputNumber(this.gng.getMaxNodes)
    .setHint("Maximum number of units")
    .setMin(10)
    .setStep(10)

  private val lambdaInput: InputNumber = new InputNumber(this.gng.getLambda)
    .setHint("Number of 'seen' signals before creating a new unit")
    .setMin(1)
    .setStep(10)

  private val maxAgeInput: InputNumber = new InputNumber(this.gng.getMaxAge)
    .setHint("Maximum age an edge can have before removing it (a higher number will foster more edges to remain)")
    .setMin(1)
    .setStep(1)

  private val epsBInput: InputNumber = new InputNumber(this.gng.getEpsB)
    .setHint("Learning rate for the winning unit")
    .setMin(.0)
    .setMax(1)
    .setStep(.1)

  private val epsNInput: InputNumber = new InputNumber(this.gng.getEpsN)
    .setHint("Learning rate for the neighbors of the winning unit")
    .setMin(.0)
    .setMax(1)
    .setStep(.1)

  private val alphaInput: InputNumber = new InputNumber(this.gng.getAlpha)
    .setHint("Error reduction rate for the neighbors of a newly created unit")
    .setMin(.0)
    .setMax(1)
    .setStep(.1)

  private val dInput: InputNumber = new InputNumber(this.gng.getD)
    .setHint("Error reduction rate for all units")
    .setMin(.0)
    .setMax(1)
    .setStep(.1)

  private val minNetSizeInput: InputNumber = new InputNumber(this.gng.getMinNetSize)
    .setHint("Tell GNG to let bigger nets absorb those of this given size (0 = any size)")
    .setMin(0)
    .setMax(5)
    .setStep(1)

  /**
    * Text used for displaying the current status of the training.
    * i.e. iteration #, percentage, nodes and edges.
    * */
  private val statusText: Text = new Text("Click Run to start training").setAttribute("class", "text-muted")


  // ---------- Event listeners -------------
  // Note: Zeppelin-Angular onchange event listeners seem to be broken, so only onclick are used.

  this.executionButton
    .setOnClickListener(() => {
      this.isTraining = !this.isTraining

      if (this.isTraining) {
        executionButton.set("Pause")
        if (epochs <= 0) this.statusText.set("Running first epoch...")
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
    this.sampleSize = this.maxSignalsInput.get
    this.gng
      .setMaxNodes(this.maxNodesInput.get.toInt)
      .setMaxAge(this.maxAgeInput.get.toInt)
      .setLambda(this.lambdaInput.get.toInt)
      .setEpsB(this.epsBInput.get)
      .setEpsN(this.epsNInput.get)
      .setAlpha(this.alphaInput.get)
      .setD(this.dInput.get)
      .setUntangle(this.untangleInput.get)
      .setMinNetSize(this.minNetSizeInput.get.toInt)
  })

  private def preprocessData(): Unit = {
    logger.info("Initializing input data (assembling training features and statistics)")

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

      if (inputCols.lengthCompare(this.maxDisplayFeatures) <= 0) {
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

    this.isTraining = false
    this.epochs = 0
    this.accTime = .0
    this.model = GNGModel(this.rdd)

    updateGraph()
    this.statusText.set("Click Run to start training")
  }


  /**
    * Show elements in notebook.
    * */
  def display: this.type = {
    logger.info("Displaying visual GNG")
    preprocessData()

    <div class="container">
      <div class="row">
        <div class="col col-lg-4 btn-group-btn" style="min-width: 200px">
          { executionButton.elem }
          { refreshButton.elem }
            <button type="button" class="btn btn-default dropdown-toggle btn-sm" data-toggle="dropdown">
            <span class="glyphicon glyphicon-cog"> GNG</span>
          </button>
          <ul class="dropdown-menu" style="padding: 5px" role="menu" >

            <li class="dropdown-header" onclick="event.stopPropagation();">Untangled
              <span class="glyphicon glyphicon-info-sign" title={ untangleInput.hint }>&nbsp;</span>
              { untangleInput.elem }
            </li>

            <li class="dropdown-header">Max epochs
              <span class="glyphicon glyphicon-info-sign" title={ maxEpochsInput.hint }></span>
            </li>
            <li class="input-group-sm" onclick="event.stopPropagation();">{ maxEpochsInput.elem }</li>

            <li class="dropdown-header">Sample signals
              <span class="glyphicon glyphicon-info-sign" title={ maxSignalsInput.hint }></span>
            </li>
            <li class="input-group-sm" onclick="event.stopPropagation();">{ maxSignalsInput.elem }</li>

            <li class="dropdown-header">Max nodes
              <span class="glyphicon glyphicon-info-sign" title={ maxNodesInput.hint }></span>
            </li>
            <li class="input-group-sm" onclick="event.stopPropagation();">{ maxNodesInput.elem }</li>

            <li class="dropdown-header">Lamda
              <span class="glyphicon glyphicon-info-sign" title={ lambdaInput.hint }></span>
            </li>
            <li class="input-group-sm" onclick="event.stopPropagation();">{ lambdaInput.elem }</li>

            <li class="dropdown-header">Max edge age
              <span class="glyphicon glyphicon-info-sign" title={ maxAgeInput.hint }></span>
            </li>
            <li class="input-group-sm" onclick="event.stopPropagation();">{ maxAgeInput.elem }</li>

            <li class="dropdown-header">eps b
              <span class="glyphicon glyphicon-info-sign" title={ epsBInput.hint }></span>
            </li>
            <li class="input-group-sm" onclick="event.stopPropagation();">{ epsBInput.elem }</li>

            <li class="dropdown-header">eps n
              <span class="glyphicon glyphicon-info-sign" title={ epsNInput.hint }></span>
            </li>
            <li class="input-group-sm" onclick="event.stopPropagation();">{ epsNInput.elem }</li>

            <li class="dropdown-header" style="display: None">alpha
              <span class="glyphicon glyphicon-info-sign" title={ alphaInput.hint }></span>
            </li>
            <li  class="input-group-sm" onclick="event.stopPropagation();" style="display: None">{ alphaInput.elem }</li>

            <li class="dropdown-header" style="display: None">d
              <span class="glyphicon glyphicon-info-sign" title={ dInput.hint }></span>
            </li>
            <li class="input-group-sm" onclick="event.stopPropagation();" style="display: None">{ dInput.elem }</li>

            <li class="dropdown-header">Min net size
              <span class="glyphicon glyphicon-info-sign" title={ minNetSizeInput.hint }></span>
            </li>
            <li class="input-group-sm" onclick="event.stopPropagation();">{ minNetSizeInput.elem }</li>

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

    try {
      while (this.isTraining) {
        /** Sampler (S). */
        val sample = this.rdd.sample(true, this.sampleSize)

        val time = Utils.performance {
          /** Optimizer (O). */
          this.model = gng.fit(sample, this.model)
        }

        accTime += time
        epochs += 1

        /** Report learning state (El). */
        updateStats()

        /** Visualization Transformation (U). */
        updateGraph()

        /** Wait at least .1 seconds when updates are too fast for user involvement. */
        if (time < .1) {
          Thread.sleep(1000)
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

    this.rdd.unpersist();
  }

  /**
    * Updates execution stats shown to the user.
    * */
  private def updateStats(): Unit = {
    var str = "Epochs: " + "%03d".format(this.epochs) + "/" + maxEpochs
    if (epochs > 0) str += f" (${accTime / epochs.toDouble}%1.2f s/epoch)"
    str += " | Nodes: " + "%03d".format(this.model.nodes.size)
    str += " | Edges: " + "%04d".format(this.model.edges.size)
    str += s"| Partitions: ${rdd.getNumPartitions}"

    this.statusText.set(str)
  }


  private def updateGraph(): Unit = {
    dataHub.push(this.model.toJSONString(this.features != null))
  }


  /**
    * Display parallel coordinates.
    * */
  def parallelCoordinates(): Unit = pc.display


  /**
    * Returns a DataFrame with each data point assigned to its closest unit.
    *
    * @param updateGraph if true (computationally expensive), updates the GNG graph, i.e., sets the size of the nodes based
    *                    on the number of data points each represents; if the data has labels, it assigns a label to each
    *                    node based on a majority vote (which is then painted in the graph); if the data has IDs, it adds
    *                    the ID as a label of the closest to each node (which is then shown as a hint).
    *
    * @return the original dataset with an appended column:
    * */
  def transform(updateGraph: Boolean = false): Dataset[_] = {
    val predictions = this.model.transform(df, withDistance = true)
    val unitIdCol = model.getOutputCol + ".unitId"
    val distanceCol = model.getOutputCol + ".distance"

    if (updateGraph) {
      import org.apache.spark.sql.functions._
      import org.apache.spark.sql.expressions.Window
      import spark.implicits._

      val stats: Array[(Int, Long, Double, String)] = (this.labelCol, this.idCol) match {
        case (Some(labelCol), _) =>
          predictions.groupBy(unitIdCol)
            .agg(collect_list(labelCol))
            .map{
              case Row(unitId: Int, labels: Seq[_]) =>
                val groupedLabels = labels.groupBy(identity)
                val certainty = 1 / groupedLabels.size.toDouble
                val label = groupedLabels.maxBy(_._2.size)._1
                (unitId, labels.size.toLong, certainty, label.toString)
            }
            .collect()

        case (_, Some(idCol)) =>
          val w = Window.partitionBy(unitIdCol)
          predictions
            .select(
              col(unitIdCol),
              col(distanceCol),
              col(idCol),
              count(col(idCol)).over(w).as("counts"),
              min(distanceCol).over(w).as("min_distance")
            )
            .where(s"$distanceCol = min_distance")
            .map{
              case Row(unitId: Int, _, rowId: String, counts: Long, _) =>
                (unitId, counts, .0, rowId)
            }
            .collect()

        case _ =>
          predictions
            .groupBy(unitIdCol)
            .count()
            .map(row => (row.getInt(0), row.getLong(1), .0, ""))
            .collect()
      }

      this.model.nodes.zipWithIndex.foreach{ case (n, i) =>
        stats.find(_._1 == i) match {
          case Some((_, count, certainty, label)) =>
            n.winCounter = count
            if (label.nonEmpty){
              n.setLabel(label)
              n.certainty = certainty
            }

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

  /**
    * Returns the id of the user-selected nodes.
    * */
  def getSelected: Array[Int] = {
    this.dataHub.get
      .tail.replace("]", "")
      .split(",")
      .map(_.toInt)
  }

  /**
    * Assigns a group name to selected nodes.
    * */
  def groupSelected(groupName: String): Unit = {
    this.getSelected.foreach(i => {
      this.model.nodes(i).group = Some(groupName)
      this.model.nodes(i).label = Some(groupName)
    })

    this.updateGraph()
  }

  /**
    * Saves the current prototype vectors as images.
    * */
  def saveAsImages(imgWidth: Int, imgHeight: Int, channels: Int, folder: String): Unit = {
    this.model.saveAsImages(imgWidth, imgHeight, channels, folder)
  }

  def saveAsJSON(file: String): Unit = this.model.saveAsJSON(file)

  def saveAsGML(file: String): Unit = this.model.saveAsGML(file)

  def savePrototypes(file: String): Unit = this.model.savePrototypes(file)

}

private case class NodeSchema(id: Int, centroid: SparkVector)

object VisualGNG {
  private var nextId = 0

  def apply(df: DataFrame): VisualGNG = {
    nextId += 1
    new VisualGNG(nextId, df)
  }
}
