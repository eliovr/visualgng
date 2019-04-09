# VisualGNG
An updated implementation of the Visual Growing Neural Gas for exploratory data analysis, as describe in  [Ventocilla et al., 2019](https://www.scitepress.org/PublicationsDetail.aspx?ID=la6GQgbV48M=&t=1).
The library also provides a plain implementation of the Growing Neural Gas ([Fritzke, 1995](http://papers.nips.cc/paper/893-a-growing-neural-gas-network-learns-topologies.pdf)).

NOTE: Both GNG and VisualGNG put some burden on the driver.

## Author
- Elio Ventocilla 
 

## Requirements
### For both GNG and VisualGNG
- [Apache Spark](http://spark.apache.org/) (v2.4)
 

### For VisualGNG
- [Apache Zeppelin](https://zeppelin.apache.org/) (v0.8)
 

## Reference
```
@inproceedings{ventocilla2019,
  title={Visual Growing Neural Gas for Exploratory Data Analysis},
  author={Ventocilla, Elio and Riveiro, Maria},
  booktitle={14th International Joint Conference on Computer Vision, Imaging and Computer Graphics Theory and Applications, 25-27 February, 2019, Prague, Czech Republic},
  volume={3},
  pages={58--71},
  year={2019},
  organization={SciTePress}
}
```

## Building
```
mvn package
```


## Usage
To deploy it in Zeppelin, go to the Spark interpreter settings and add a new dependency artifact with the path to the generated jar file (e.g. /path/to/visualgng/target/visualgng-1.0-SNAPSHOT.jar)
Once the dependency is added, it should be possible to import the needed packages from a notebook.

An example using the [Iris dataset](https://archive.ics.uci.edu/ml/datasets/Iris) from the UCI repository:


```scala
import se.his.sail.zeppelin.VisualGNG

val df = spark.
    read.
    option("header", true).
    option("inferSchema", true).
    csv("/path/to/iris.data")
    
val gng = new VisualGNG(df).
    setLabelCol("label").
    display
```

The `display` method will display the visual elements as an output of the current notebook paragraph, and return a VisualGNG object.
By default, VisualGNG assumes that all columns in the dataset will be used in training.

### Parameters
- `setInputCols`: takes an `Array[String]`. It defines which columns to be used in training.
- `setLabelCol`: takes a string. It tells the library that to treat the given column as a label. This means that the column is ignored during training, but the units will assume the color of the latest win. This column can be of type Int, Double or String.
- `setIdCol`: taked a string. Similar to `setLabelCol` but it won't be visually encoded. If `setInputCols` is not given, then all other columns will be used for training. This column can be of any type.
- `setScale`: takes a boolean. It defines whether the training features should be scaled to a unit standard deviation or not.


### VisualGNG object
The instantiated VisualGNG object can be used in other paragraphs for different purposes. Following the previous example with the `gng` object:

- `gng.parallelCoodinates()`: will display a parallel coordinates plot representing the units in the GNG model. This plot dynamically updates along with the GNG's force-directed graph.
- `gng.kmeans`: takes a number for k as an integer and returns a `KMeansModel`. Will run K-means on the trained units' prototypes and visually encode the result in the force-directed graph.
- `gng.computeDensity()`: will compute the true density and update the nodes in the graph accordingly.
- `gng.model`: returns the `GNGModel` instance. This can be used as a transformer (in the SparkML sense) on new data.
- `gng.getPredictions`: takes a boolean (optional, default = false) and returns a `DataFrame`. It will apply the original data to the trained model and assign units' ids to each data point. If the true is given, then it will also provide the Euclidean distance to the corresponding unit.