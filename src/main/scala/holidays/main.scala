package holidays


object Utils {
    def printOk(s: String) = {
        Console.GREEN
        println(s)
        Console.WHITE
    }

    def printKo(s: String) = {
        Console.RED
        println(s)
        Console.WHITE
    }
}

object Main {
  // zip((1,2,3), (a,b,c)) => ((1,a), (2,b), (3,c))
    def zip[A, B](a: List[A], b: List[B]): List[(A,B)] = (a, b) match {
      case (Nil, Nil) => Nil
      case (x::t1, y::t2) => (x,y)::zip(t1, t2)
      case (_,_) => throw new IllegalArgumentException
    }

    def main(args: Array[String]) {
      if (args.length != 3){
        println("java holidays.jar AIRPORTS.csv COUNTRIES.csv RUNWAYS.csv")
      }
      else {
        var csvNames = List[String]("airports", "countries", "runways")
        var csvList: List[Csv] = zip(args.toList, csvNames)
                                  .map(x =>  Parse_csv.readCsv(x._1, x._2))
        println("----------------------------------------")
        //csvList(0).getAllCols("iso_region")
        //csvList(0).getLinesMatchingCol("ident", "ZZZZ").map(x => println(x))
        //println(csvList(0).getData)
        println("----------------------------------------")
        //println(csvToElastic.toJsonSimple(csvList(0).getCols, csvList(0).getData(0)))
        csvToElastic.csvToElastic(csvList(0))
      }
  }
}
