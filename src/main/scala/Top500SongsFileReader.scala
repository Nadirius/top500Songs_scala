import java.io.File
import java.util.Scanner
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import scala.io.Source


object Top500SongsFileReader {
  def apply():Iterator[Array[String]] = {
Source
      .fromFile(new File("src/public/top500Songs.csv")).getLines.drop(1) // drop header (first line)
      .map(raw ⇒ raw.split(";")).map(raw ⇒
        raw.zipWithIndex.map { case (col, ind) ⇒
              format(ind)(col)
          }
      )
  }

  val format:Int ⇒ String ⇒ String = new Function1[Int, Function1[String, String]] {
    override def apply(index: Int): String ⇒ String = {
      index match {
        case 1 ⇒ new Function1[String, String] {
          override def apply(data: String): String = {
            encodingCorrections(data.substring(1))
          }
        }
        case 6 ⇒ new Function1[String, String] {
          override def apply(data: String): String = {
            datePatternCorrections(encodingCorrections(data))
          }
        }
        case 7 ⇒ new Function1[String, String] {
          override def apply(data: String): String = {
            streakCorrections(encodingCorrections(data))
          }
        }
        case 8 ⇒ new Function1[String, String] {
          override def apply(data: String): String = {
            rankingCorrections(encodingCorrections(data))
          }
        }
        case _ ⇒ new Function1[String, String] {
          override def apply(data: String): String = {
            encodingCorrections(data)
          }
        }
      }
    }
  }

  val encodingCorrections: (String ⇒ String) = new Function1[String, String] {
    override def apply(data: String): String =
      data
        .trim()
        .replace("â€™", "'")
        .replace("â€”", "-")
        .replace("â€¦", "…")
        .replace("â€œ", "“")
        .replace("â€”", "—")
        .replace("â€“", "–")
        .replace("â€˜", "‘")
        .replace("Ã–", "Ö")
        .replace("Ã¶", "ö")
        .replace("Ã¨", "è")
        .replace("Ã©", "é")
        .replaceAll("â€.", "")
        .replaceAll("\"\"+", "\"")

  }

  val datePatternCorrections: String ⇒ String = new Function1[String, String] {
    override def apply(data: String): String = {
      var temp = data.split(",")
      val formatFull = DateTimeFormatter.ofPattern("yyyy-MMMM-dd")
      val format = DateTimeFormatter.ofPattern("yyyy-MMM-dd")
      val mounth = if (temp(0).replace(".", "").trim() == "Sept") "Sep" else temp(0).replace(".", "").trim()
      val year = temp(1).trim()
      try {
        LocalDate.parse(s"${year}-${mounth}-01", format).toString
      }
      catch {
        case e: Throwable ⇒ {
          LocalDate.parse(s"${year}-${mounth}-01", formatFull).toString
        }
      }
    }
  }


  val streakCorrections:String ⇒ String = new Function1[String, String] {
    override def apply(data: String): String = {
      if (data.trim() != "none" && data.trim() != "Non-Single" && data.trim() != "Did not chart" && data.trim() != "Non-single in the U.S." && data.trim() != "Non-single in U.S." && data.trim() != "Predates chart" && data.trim() != "Predates pop charts" && data.trim() != "Did Not Chart") {
        data
          .trim()
          .split(" ")(0)
      } else {
        data
      }
    }
  }

  val rankingCorrections: String ⇒ String = (data: String) => {
    if (data.trim() != "none") {
      data
        .trim()
        .split("\\.")(1)
        .trim()
    } else {
      "none"
    }
  }
}











