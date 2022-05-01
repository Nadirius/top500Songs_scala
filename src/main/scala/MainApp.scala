import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}

object MainApp extends App{

  //val spark = SparkSession.builder().getOrCreate()

  val top500songsList:List[Top500SongsObject.Song] = Top500SongsFileReader()

  val top500SongsClassified = top500songsList.filter(s ⇒ s.position.matches("\\d+"))
  val top500SongsUnclassified = top500songsList.filter(s ⇒ !s.position.matches("\\d+"))

  top500SongsUnclassified.foreach(_.display())

//  val df = spark.createDataFrame(
//    spark.sparkContext.parallelize(Top500SongsFileReader()), new StructType().add("TITLE", StringType))
//  )


  AppMenu.displayMenu()

  val a = scala.io.StdIn.readInt()

  println(a)

  // AppMenu.handleUserInput(scala.io.StdIn.readInt())



}
