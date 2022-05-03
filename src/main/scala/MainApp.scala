import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

import scala.concurrent.Future

import java.time.LocalDate


object MainApp extends App {
  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  var classifiedFlag: Boolean = true
  var unclassifiedFlag: Boolean = true

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]").appName("MaNa_top500songs").getOrCreate()

  import spark.implicits._

  var classified: RDD[(String, String, String, String, String, String, LocalDate, Int, Int)] = null
  var unclassified: RDD[(String, String, String, String, String, String, LocalDate, String)] = null

  var classifiedDf: sql.DataFrame = null
  var unclassifiedDf: sql.DataFrame = null

  def createUnclassifiedContext(get: Seq[(String, String, String, String, String, String, LocalDate, String)]) = spark.sparkContext.parallelize(get)
  def createClassifiedContext(get: Seq[(String, String, String, String, String, String, LocalDate, Int, Int)]) = spark.sparkContext.parallelize(get)

  def convertToUnClassifiedDataframe(get: RDD[(String, String, String, String, String, String, LocalDate, String)]): sql.DataFrame = get.toDF("title", "description", "appears_on", "artist", "writers", "producer", "released", "comment")
  def convertToClassifiedDataframe(get: RDD[(String, String, String, String, String, String, LocalDate, Int, Int)]): sql.DataFrame = get.toDF("title", "description", "appears_on", "artist", "writers", "producer", "released", "streak", "position")


  val getClassifiedSongs: () ⇒ Seq[(String, String, String, String, String, String, LocalDate, Int, Int)] = () => Top500SongsFileReader().filter(s ⇒ s(8).matches("\\d+"))
    .map { case Array(title, description, appears_on, artist, writers, producer, released, streak, position) ⇒
      (title, description, appears_on, artist, writers, producer, LocalDate.parse(released), streak.toInt, position.toInt)
    }
    .toSet
    .toSeq

  val getUnClassifiedSongs: () ⇒ Seq[(String, String, String, String, String, String, LocalDate, String)] = () => Top500SongsFileReader().filter(s ⇒ !s(8).matches("\\d+"))
    .map { case Array(title, description, appears_on, artist, writers, producer, released, streak, position) ⇒
      (title, description, appears_on, artist, writers, producer, LocalDate.parse(released), streak)
    }
    .toSet
    .toSeq


  Future(getClassifiedSongs()).onComplete(songs ⇒
    Future(createClassifiedContext(songs.get)).onComplete(ctx =>
      Future(convertToClassifiedDataframe(ctx.get)).onComplete(dataframe ⇒ {
        classifiedDf = dataframe.get
        println("Classified songs ready")
        classifiedFlag = false
      })))

  Future(getUnClassifiedSongs()).onComplete(songs ⇒
    Future(createUnclassifiedContext(songs.get)).onComplete(ctx =>
      Future(convertToUnClassifiedDataframe(ctx.get)).onComplete(dataframe ⇒ {
        unclassifiedDf = dataframe.get
        println("UnClassified songs ready")
        unclassifiedFlag = false
      })))

  println("loading data, please wait.")
  while (classifiedFlag || unclassifiedFlag) {
    Thread.sleep(200)
  }
  println()

  unclassifiedDf.printSchema()
  unclassifiedDf.show(Int.MaxValue, 25)

  classifiedDf.printSchema()
  classifiedDf.show(Int.MaxValue, 25)

  classifiedDf.sort($"position",$"streak".desc).show(Int.MaxValue, 40)


  AppMenu.displayMenu()

  var userInputMenu = 1000

  def handleUserInput(userInputMenu: Int) = {
    case 1 ⇒  classifiedDf.sort($"position",$"streak".desc).show(Int.MaxValue, 40)
    case 2 ⇒ classifiedDf
  }

  while(userInputMenu != 0) {
    userInputMenu = scala.io.StdIn.readInt()
    handleUserInput(userInputMenu)
  }

  sys.exit(0)


}
