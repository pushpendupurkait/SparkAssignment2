import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object assignment2 extends App {
  val conf = new SparkConf().setAppName("assignment2").setMaster("local")
  val sc = new SparkContext(conf)

  val spark = SparkSession.builder().appName("assignment2").getOrCreate()
  import spark.implicits._

  val df: DataFrame = spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .load("/home/pushpendu/Desktop/Fire_Department_Calls_for_Service.csv")

  val first = df.map(x => x.getString(3)).distinct.count
  println(s"First answer = ${first}")

  val second = df.select(df("Call Type")).rdd.map(x => (x,1)).reduceByKey(_ + _).collect()
  println(s"Second answer = ${second}")

  val minYear = df.map(_.getString(4))
    .map(_.split("/"))
    .map(x => x(2).toInt).rdd.min
  val maxYear = df.map(_.getString(4))
    .map(_.split("/"))
    .map(x => x(2).toInt).rdd.max
  val third  = maxYear - minYear
  println(s"Third answer = ${third}")


  df.createOrReplaceTempView("fire_dept")
  val fourth = spark.sql("SELECT count(*) from fire_dept where 'Call Date' between (select DATE_SUB(MAX('Call Date'), 7) from fire_dept) AND (SELECT MAX('Call Date') from fire_dept)").show
  println(s"Fourth answer = ${fourth}")


  val fifth = df.map(_.getString(4)).map(_.split("/"))
    .map(x => x(2).toInt)
    .filter(x => x == 2015).count
  println(s"Fifth answer = ${fifth}")
}
