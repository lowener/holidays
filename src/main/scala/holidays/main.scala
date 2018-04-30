package holidays


object Utils {
    def printOk(s: String) = {
        println(Console.GREEN + s + Console.WHITE)
    }

    def printKo(s: String) = {
        println(Console.RED + s + Console.WHITE)
    }
}

object Main {
  // zip((1,2,3), (a,b,c)) => ((1,a), (2,b), (3,c))
    def zip[A, B](a: List[A], b: List[B]): List[(A,B)] = (a, b) match {
      case (Nil, Nil) => Nil
      case (x::t1, y::t2) => (x,y)::zip(t1, t2)
      case (_,_) => throw new IllegalArgumentException
    }


    def textUserInterface = {
      println("Welcome to your holiday planner. What type of information do you want?")
      println("1. Query")
      println("2. Report")
      try {
         val queryOrReport = readInt

         if (queryOrReport == 1) {
            println("Query about a country. Do you want to use:")
            println("1. Country code?")
            println("2. Country name?")
            val codeOrName = readInt
            println("Enter the country")
            val country : String = readLine
            val code = codeOrName match {
               case c if c == 1 => Elastic.searchCountry("code", country)
               case c if c == 2 => Elastic.searchCountry("name", country)
               case _=> {
                  println("Invalid answer")
                  None
               }
            }
            code match {
               case Some(countryCode) =>Elastic.searchAirportsByCountry(countryCode("code").toString)
               case None => ()
            }
         }
         else  if (queryOrReport == 2){
            Elastic.reportAirports
            Elastic.reportTop10MostCommonRunwayLatitude()
            Elastic.reportRunways()
            println("Not implemented yet")
         }
         else {
            println("Invalid answer")
         }
      } catch {
          case e: NumberFormatException => println("Error on input!")
      }
   }

    def main(args: Array[String]) {
      if (args.length != 3){
        println("java holidays.jar AIRPORTS.csv COUNTRIES.csv RUNWAYS.csv")
      }
      else {
         var csvNames = List[String]("airports", "countries", "runways")
         var csvList: List[Csv] = zip(args.toList, csvNames)
                                  .map(x =>  Parse_csv.readCsv(x._1, x._2))

         csvList.foreach(csv => {
            if (csvToElastic.createESIndexIfExists(csv.getName))
               csvToElastic.csvToElasticGrouped(csv)
         })
         //Elastic.searchCountry("name", "Frince")
         textUserInterface
         //Elastic.reportAirports
         ElasticClient.client.close()
      }
  }
}
