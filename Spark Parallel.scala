// Databricks notebook source
// MAGIC %md # Spark, Parallelising the Parallel Jobs

// COMMAND ----------

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

// COMMAND ----------

// MAGIC %sql show tables in default

// COMMAND ----------

val building_permits= spark.read.table("default.building_permits")
val covid_19_data= spark.read.table("default.covid_19_data")
val salaries= spark.read.table("default.salaries")
val scrubbed= spark.read.table("default.scrubbed")
val table_13reasonswhy= spark.read.table("default.table_13reasonswhy")

// COMMAND ----------

val building_permits_filter= building_permits.filter($"Permit Type Definition" === "otc alterations permit").count()
val covid_19_data_filter=covid_19_data.filter($"confirmed" >= 20).count
val salaries_filter=salaries.filter($"BasePay" >= 248895.7).count
val scrubbed_filter=scrubbed.filter($"shape" === "cylinder").count
val table_13reasonswhy_count=table_13reasonswhy.count()

// COMMAND ----------

// import spark functions and date time for logging

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util
import java.util.Calendar
import org.apache.spark.sql.functions.{col,sum}

val log_format = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
val log_time = log_format.format(Calendar.getInstance().getTime())

// COMMAND ----------

// A common function which is applicable for all the tables given
// It does
// 1. gets the table count
// 2. unions the same dataframe and count the records
// 3. validates each of the columns in the dataframe and gives you the count of records of nulls.

def commonCountSQLFunction(DBNAME:String,TABLE_NAME:String): org.apache.spark.sql.DataFrame ={
  val table_df=spark.table(DBNAME+"."+TABLE_NAME)
  table_df.union(table_df).count
  val table_count=table_df.count
  val table_validation=table_df.select(table_df.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*)
  println(log_format.format(Calendar.getInstance().getTime())+" Count of "+DBNAME+"."+TABLE_NAME+":" + table_count)
  println(log_format.format(Calendar.getInstance().getTime())+" "+table_validation)
  table_validation
}

// COMMAND ----------

// Running the common functions for all the tables.
// make a note that it is running the function in parallelly.

val DB_NAME="default"
var TABLE=""

TABLE="building_permits"
println(log_format.format(Calendar.getInstance().getTime())+" Triggering sequential job for: "+TABLE)
commonCountSQLFunction(DB_NAME,TABLE)

TABLE="covid_19_data"
println(log_format.format(Calendar.getInstance().getTime())+" Triggering sequential job for: "+TABLE)
commonCountSQLFunction(DB_NAME,TABLE)

TABLE="salaries"
println(log_format.format(Calendar.getInstance().getTime())+" Triggering sequential job for: "+TABLE)
commonCountSQLFunction(DB_NAME,TABLE)

TABLE="scrubbed"
println(log_format.format(Calendar.getInstance().getTime())+" Triggering sequential job for: "+TABLE)
commonCountSQLFunction(DB_NAME,TABLE)

TABLE="table_13reasonswhy"
println(log_format.format(Calendar.getInstance().getTime())+" Triggering sequential job for: "+TABLE)
commonCountSQLFunction(DB_NAME,TABLE)

// COMMAND ----------

// Running the common functions for all the tables.
// make a note that it is running the function in parallelly.
val TABLES="building_permits,covid_19_data,salaries,scrubbed,table_13reasonswhy"
val DB_NAME="default"

val table_list=TABLES.split(",").toList
for(tbl <- table_list){
  println(log_format.format(Calendar.getInstance().getTime())+" Triggering sequential job for:"+tbl)
  commonCountSQLFunction(DB_NAME,tbl)
}

// COMMAND ----------

import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}


def runSampleFunctionParallel(DBNAME:String, TABLES:String): Unit = {
			val table_list=TABLES.split(",").toList
			println(log_format.format(Calendar.getInstance().getTime())+" ThreadPoolCount: " + table_list.length)
			implicit val executionContext = ExecutionContext.fromExecutorService(
				Executors.newFixedThreadPool(table_list.length))

			try {
				println(log_format.format(Calendar.getInstance().getTime())+"DBNAME Name: " + DBNAME)
				println(log_format.format(Calendar.getInstance().getTime())+"#####################################")
				println("TABLE LIST: "+TABLES)

				val futures = table_list.zipWithIndex.map { case (tbl, i) =>
					Future({
                      println(log_format.format(Calendar.getInstance().getTime())+" Triggering parallel job for: "+tbl)
						spark.sparkContext.setLocalProperty("spark.scheduler.pool", s"pool${i % table_list.length}")
						commonCountSQLFunction(DBNAME,tbl)
					})}

				Await.result(Future.sequence(futures), atMost = Duration.Inf)

			}
			catch {
				case e: Exception =>
					e.printStackTrace()
					println(log_format.format(Calendar.getInstance().getTime())+" [" + DBNAME + ":" + table_list + "] ERROR: runSampleFunctionParallel: Fix the error and rerun the job")
			}
			finally {
				// ensure to clean up the threadpool
				executionContext.shutdownNow()
            }
}

// COMMAND ----------

val TABLES="building_permits,covid_19_data,salaries,scrubbed,table_13reasonswhy"
val DB_NAME="default"

runFunctionParallel(DB_NAME,TABLES)

// COMMAND ----------


