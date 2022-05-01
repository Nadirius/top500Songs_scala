import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}

import java.time.LocalDate



object MainApp extends App {
  val spark:SparkSession = SparkSession.builder()
    .master("local[1]").appName("MaNa_top500songs").getOrCreate()

  import spark.implicits._

  val  top500SongsUnclassified = Top500SongsFileReader().filter(s ⇒ !s(8).matches("\\d+"))
    .map{ case Array(title,description,appears_on,artist,writers,producer,released,streak,position) ⇒
    (title,description,appears_on,artist,writers,producer,LocalDate.parse(released),streak) }
    .toSet
    .toSeq

  val top500SongsClassified  = Top500SongsFileReader().filter(s ⇒ s(8).matches("\\d+"))
    .map{ case Array(title,description,appears_on,artist,writers,producer,released,streak,position) ⇒
    (title,description,appears_on,artist,writers,producer,LocalDate.parse(released),streak.toInt,position.toInt) }
    .toSet
    .toSeq

  val classified = spark.sparkContext.parallelize(top500SongsClassified)
  val unclassified = spark.sparkContext.parallelize(top500SongsUnclassified)


  val unclassifiedDf = unclassified.toDF("title","description","appears_on","artist","writers","producer","released","comment")
  //top500SongsClassifiedDataFrame.printSchema()
  unclassifiedDf.show(Int.MaxValue, Int.MaxValue, true)

  val classifiedDf = classified.toDF("title","description","appears_on","artist","writers","producer","released","streak","position")
  //top500SongsClassifiedDataFrame.printSchema()
  classifiedDf.show(Int.MaxValue, Int.MaxValue, true)









  import spark.implicits._



  //top500SongsUnclassified.foreach(_.display())



  //val classifiedDataFrame = spark.createDataFrame(spark.sparkContext.parallelize(top500SongsClassified), classifiedSongsStructureSchema)
  //classifiedDataFrame.





  //AppMenu.displayMenu()

  //val a = scala.io.StdIn.readInt()

  //println(a)

  // AppMenu.handleUserInput(scala.io.StdIn.readInt())


}
