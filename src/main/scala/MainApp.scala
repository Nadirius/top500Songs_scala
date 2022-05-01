

object MainApp extends App{

  val top500songsList:List[Top500SongsObject.Song] = Top500SongsFileReader()

  top500songsList.foreach(_.display())

  AppMenu.displayMenu()

  val a = scala.io.StdIn.readInt()

  println(a)

  // AppMenu.handleUserInput(scala.io.StdIn.readInt())



}
