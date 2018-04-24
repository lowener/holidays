package holidays




class Csv(nameCols: List[String], data: List[List[String]]) {
    def printName = {
        println(nameCols.length)
        nameCols.map(x => println(x))
    }

    def getCols(col: String): List[String] = {
        val colNb = nameCols.indexOf(col)
        if (colNb == -1) {
            println("Column " + col + " not found")
        }
        val values : List[String] = data:transform(lines => lines(colNb))
    }
}
