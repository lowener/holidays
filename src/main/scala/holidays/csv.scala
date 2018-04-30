package holidays


class Csv(name: String, cols: List[String], data: List[List[String]]) {
    def print = {
      println(cols.length)
      cols.map(x => println(x))
    }

    // Getters
    def getName = name
    def getCols = cols
    def getData = data

    def getAllCols(col: String): List[String] = {
        val colNb = cols.indexOf(col)
        if (colNb == -1) {
            println("Column " + col + " not found")
            Nil
        }
        else {
            data.foldLeft(List[String]()){
              (acc, elt) => elt(colNb)::acc
            }
          }
    }

    def getLinesMatchingCol(col: String, value: String): List[List[String]] = {
      val colNb = cols.indexOf(col)
      if (colNb == -1) {
          println("Column " + col + " not found")
          Nil
      }
      else {
          data.filter(x => x(colNb) == value)
        }
    }
}
