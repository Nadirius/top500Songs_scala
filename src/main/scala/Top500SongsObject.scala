import Top500SongsObject.Displayable

import java.time.LocalDate

object Top500SongsObject {

  trait Displayable {
    def display(): Unit
  }

  case class Song(title:String, description:String, appearsOn : String, artist: String, writers:String, producer:String, released:String)

  class Classified(override val title:String, override val description:String, override val appearsOn : String, override val artist: String, override val writers:String, override val producer:String, override val released:String, streak:String, position:String) extends Song(title:String, description:String, appearsOn : String, artist: String, writers:String, producer:String, released:String) with Displayable
  {
    override def display() : Unit = println(
      s"""
TITLE : $title
    DESCRIPTION : $description
    APPEARS ON : $appearsOn
    ARTIST : $artist
    WRITERS : $writers
    PRODUCER : $producer
    RELEASED : $released
    STREAK : $streak weaks
    POSITION : N° $position
    """)
  }
  class UnClassified(override val title:String, override val description:String, override val appearsOn : String, override val artist: String, override val writers:String, override val producer:String, override val released:String,comment:String) extends Song(title:String, description:String, appearsOn : String, artist: String, writers:String, producer:String, released:String) with Displayable
  {
    override def display() : Unit = println(
      s"""
TITLE : $title
    DESCRIPTION : $description
    APPEARS ON : $appearsOn
    ARTIST : $artist
    WRITERS : $writers
    PRODUCER : $producer
    RELEASED : $released
    COMMENT : $comment
    """)
  }

}
