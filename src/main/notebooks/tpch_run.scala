// Databricks notebook source
// TPCH runner (from spark-sql-perf) to be used on existing tables
// edit the main configuration below

val scaleFactors = Seq(100) //set scale factors to run
val workers = 3 // Edit to the number of worker
val baseLocation = "/data1/liangliang/benchmark/data" // S3 bucket, blob, or local root path
val resultLocation = "/data1/liangliang/benchmark/data/results"
val fileFormat = "parquet" // only parquet was tested
val overwrite = true
val format = "parquet" //format has have already been generated

val dbSuffix = "" // set only if creating multiple DBs or source file folders with different settings, use a leading _

def perfDatasetsLocation(scaleFactor: Int, format: String) = 
  s"$baseLocation/tpch/sf${scaleFactor}_${format}"

val iterations = 2
def databaseName(scaleFactor: Int, format: String) = s"tpch_sf${scaleFactor}_${format}"
val randomizeQueries = false //to use on concurrency tests

// Experiment metadata for results, edit if outside Databricks
val configuration = "default" //use default when using the out-of-box config
val runtype = "TPCH run" // Edit
val workerInstanceType = "my_VM_instance" // Edit to the instance type

// Make sure spark-sql-perf library is available (use the assembly version)
import com.databricks.spark.sql.perf.Tables
import com.databricks.spark.sql.perf.tpch._
import org.apache.spark.sql.functions._

// default config (for all notebooks)
var config : Map[String, String] = Map (
  "spark.sql.broadcastTimeout" -> "7200" // Enable for SF 10,000
)
// Set the spark config
for ((k, v) <- config) spark.conf.set(k, v)
// Print the custom configs first
for ((k,v) <- config) println(k, spark.conf.get(k))
// Print all for easy debugging
print(spark.conf.getAll)

val tpch = new TPCH(sqlContext = spark.sqlContext)

// filter queries (if selected)
import com.databricks.spark.sql.perf.Query
import com.databricks.spark.sql.perf.ExecutionMode.CollectResults
import org.apache.commons.io.IOUtils

val queries = (1 to 22).map { q =>
  val queryContent: String = IOUtils.toString(
    getClass().getClassLoader().getResourceAsStream(s"tpch/queries/$q.sql"))
  new Query(s"Q$q", spark.sqlContext.sql(queryContent), description = s"TPCH Query $q",
    executionMode = CollectResults)
}

// Create the DB, import data, create
def createExternal(location: String, dbname: String, tables: Tables) = {
  tables.createExternalTables(location, fileFormat, dbname, overwrite = overwrite, discoverPartitions = true)
}

// Generate the data, import the tables, generate stats for selected benchmarks and scale factors
scaleFactors.foreach { scaleFactor => {
  val tables = new TPCHTables(spark.sqlContext, "", scaleFactor = s"$scaleFactor", useDoubleForDecimal = false, useStringForDate = false, generatorParams = Nil)
  val location = s"$baseLocation/tpch/sf${scaleFactor}_${fileFormat}"
  val dbname = s"tpch_sf${scaleFactor}_${fileFormat}${dbSuffix}"
  println(s"\nImporting data into DB $location from $location")
  createExternal(location, dbname, tables)
  }
}

// COMMAND ----------

scaleFactors.foreach{ scaleFactor =>
  println("DB SF " + databaseName(scaleFactor, format))
  sql(s"USE ${databaseName(scaleFactor, format)}")
  val experiment = tpch.runExperiment(
   queries,
   iterations = iterations,
   resultLocation = resultLocation,
   tags = Map(
   "runtype" -> runtype,
   "date" -> java.time.LocalDate.now.toString,
   "database" -> databaseName(scaleFactor, format),
   "scale_factor" -> scaleFactor.toString,
   "spark_version" -> spark.version,
   "system" -> "Spark",
   "workers" -> s"$workers",
   "workerInstanceType" -> workerInstanceType,
   "configuration" -> configuration
   )
  )
  println(s"Running SF $scaleFactor")
  experiment.waitForFinish(36 * 60 * 60) //36hours
  val summary = experiment.getCurrentResults
  .withColumn("Name", substring(col("name"), 2, 100))
  .withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
  .select('Name, 'Runtime)
  summary.show(9999, false)
}