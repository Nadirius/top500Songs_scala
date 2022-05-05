import Top500SongsObject.{Classified, Song, UnClassified}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.concurrent.Future
import java.time.LocalDate
import java.time.format.DateTimeFormatter


object MainApp extends App {
  //Disable spark logs
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

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

  println("\n Unclassified songs tree")
  unclassifiedDf.printSchema()

  println("\n classified songs tree")
  classifiedDf.printSchema()
  println()


  def showSongBytitle() =  {
    print("Please provide a song title : ")
    val title = scala.io.StdIn.readLine()
    classifiedDf
      .filter(raw ⇒ raw(0).toString.toLowerCase.contains(title.toLowerCase()))
      .map(raw ⇒ new Classified(raw(0).toString,raw(1).toString,raw(2).toString,raw(3).toString,raw(4).toString,raw(5).toString, raw(6).toString.substring(0,7),raw(7).toString,raw(8).toString)).foreach(_.display)
    unclassifiedDf
      .filter(raw ⇒ raw(0).toString.toLowerCase.contains(title.toLowerCase()))
      .map(raw ⇒ new UnClassified(raw(0).toString,raw(1).toString,raw(2).toString,raw(3).toString,raw(4).toString,raw(5).toString, raw(6).toString.substring(0,7),raw(7).toString)).foreach(_.display)

  }

  def showSongsByWriter() = {
  print("Please provide a writer : ")
    val writer = scala.io.StdIn.readLine()
  classifiedDf
    .filter(raw ⇒ raw(4).toString.toLowerCase.contains(writer.toLowerCase()))
    .map(raw ⇒ new Classified(raw(0).toString,raw(1).toString,raw(2).toString,raw(3).toString,raw(4).toString,raw(5).toString, raw(6).toString.substring(0,7),raw(7).toString,raw(8).toString)).foreach(_.display)
  unclassifiedDf
    .filter(raw ⇒ raw(4).toString.toLowerCase.contains(writer.toLowerCase()))
    .map(raw ⇒ new UnClassified(raw(0).toString,raw(1).toString,raw(2).toString,raw(3).toString,raw(4).toString,raw(5).toString, raw(6).toString.substring(0,7),raw(7).toString)).foreach(_.display)

  }

  def handleUserInput(userInputMenu: Int): Unit = userInputMenu match {
    case 1 ⇒ classifiedDf.sort($"position",$"streak".desc).show(Int.MaxValue, 40)
    case 2 ⇒ classifiedDf.sort($"released").show(Int.MaxValue, 40)
    case 3 ⇒ unclassifiedDf.show(Int.MaxValue,40)
    case 4 ⇒ showSongBytitle()
    case 5 ⇒ showSongsByWriter()
    case 0 ⇒  sys.exit(0)
    case _ ⇒ ""
  }


  do{
    AppMenu.displayMenu()
    handleUserInput( scala.io.StdIn.readInt())
  } while(true)




}
