import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

import scala.concurrent.Future

import java.time.LocalDate

object AppMenu {

  def displayMenu : () ⇒ Unit = () ⇒ println(
    s"""
MENU :
\tpress 1 : To view top 500 songs ordered by position then by streak
\tpress 2 : To view not classified songs
\tpress 3 : To look for a song by title
\tpress 4 : To look for a songs by writer
\tpress 5 : To view Songs by producers
""")



}
