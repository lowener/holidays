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

         val code = Elastic.searchCountry("name", "france")
         code match {
            case Some(countryCode) =>Elastic.searchAirportsbyCountry(countryCode("code").toString)
            case None => ()
         }

         ElasticClient.client.close()
      }
  }
}
