val a = Array(4, 2, 1, 3)
a.reduceLeft(_ + _)
a.reduceLeft((x,y) => x + y)

/**
  * The foldLeft method works just like reduceLeft,
  * but it lets you set a seed value to be used for the first element.
  * The following examples demonstrate a “sum” algorithm,
  * first with reduceLeft and then with foldLeft, to demonstrate the difference:
  */

a.reduceLeft(_ * _)
a.reduceLeft(_ min _)
a.reduceLeft(_ max _)

val findMax = (x: Int, y: Int) => {
  val winner = x max y
  println(s"compared $x to $y, $winner was larger")
  winner
}

a.reduceLeft(findMax)