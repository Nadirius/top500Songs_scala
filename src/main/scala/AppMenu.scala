

object AppMenu {

  def displayMenu : () ⇒ Unit = () ⇒ println(
    s"""
MENU :
\tpress 1 : To view top 500 songs ordered by position then by streak
\tpress 2 : To view top 500 songs ordered by released date
\tpress 3 : To view not classified songs
\tpress 4 : To look for a songs by title
\tpress 5 : To view Songs by writer
""")



}
