package holidays
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

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
            val codeOrName = scala.io.StdIn.readInt()
            println("Enter the country")

            val country : String = readLine
            val codeFuture = codeOrName match {
               case c if c == 1 => Elastic.searchCountry("code", country)
               case c if c == 2 => Elastic.searchCountry("name", country)
               case _=> Future {
                  println("Invalid answer")
                  None
               }
            }
            val searchAirportsRes = codeFuture.flatMap(code => code match {
               case Some(countryCode) => Elastic.searchAirportsByCountry(countryCode("code").toString)
               case None => Future{("","")}
            })
            //println(Elastic.getType(futureExec))

            try {
               val resultString : (String, String) = Await.result(searchAirportsRes, Duration("15 seconds"))
               val airportStr = resultString._1
               val countryCode = resultString._2
               println(airportStr)
               val i = readInt
               if (i > 0) {
                  val airportStr2 = Await.result(Elastic.searchAirportsByCountry(countryCode, false, i), Duration("15 seconds"))
                  println(airportStr2)
               }
            } catch {
               case e: NumberFormatException => println("Wrong input")
               case e: TimeoutException => println("Waited too long.\nExiting")
            }


            // Await here since otherwise, the program exits.
            //try {

            /*} catch {
            }*/

         }
         else  if (queryOrReport == 2) {
            println("What kind of report do you want ?")
            println("1. Top 10 countries with highest and lowest number of airports?")
            println("2. Type of runways (as indicated in \"surface\" column) per country?")
            println("3. Top 10 most common runway latitude (indicated in \"le_ident\" column)?")
            val reportKind = scala.io.StdIn.readInt()
            val resultString = reportKind match {
               case 1 => Elastic.reportAirports()
               case 2 => {
                  println("How much country do you want to print?")
                  val countryNumber = scala.io.StdIn.readInt()
                  Elastic.reportRunways(countryNumber)
               }
               case 3 => Elastic.reportTop10MostCommonRunwayLatitude()
               case _=> {
                  Future{"Invalid answer"}
               }
            }
            Await.result(resultString.map(println), Duration("25 seconds"))
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

         // Await because we cant start the TUI before the csv is imported,
         // and if we don't wait, the program exits
         val csvImported : List[Future[Any]] = csvList.map(csv => {
            csvToElastic.createESIndexIfExists(csv.getName)
                        .map(booleanRes => if (booleanRes)
                                          csvToElastic.csvToElasticGrouped(csv))

         })
         csvImported.foreach(csvFuture => Await.result(csvFuture, Duration("25 seconds")))
         //Elastic.searchCountry("name", "Frince")
         textUserInterface
         //Elastic.reportAirports
         ElasticClient.client.close()
      }
  }
}
