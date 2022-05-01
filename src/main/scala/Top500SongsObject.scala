import java.time.LocalDate

object Top500SongsObject {

  trait Displayable {
    def display(): Unit
  }
  case class Song(title:String, description:String, appearsOn : String, artist: String, writers:String, producer:String, released:Any, streak:Any, position:Any)
  extends Displayable {
    def display() : Unit = println(
      s"""
TITLE : $title
    DESCRIPTION : $description
    APPEARS ON : $appearsOn
    ARTIST : $artist
    WRITERS : $writers
    PRODUCER : $producer
    RELEASED : $released
    STREAK : $streak
    POSITION : $position
    """)



  }


}
