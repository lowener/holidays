package holidays
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http._
import scala.reflect.runtime.universe._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import java.util.concurrent.Executors
import scala.util.{Failure,Success}
import scala.concurrent.duration._

object ElasticClient{
  lazy val client = HttpClient(ElasticsearchClientUri("localhost", 9200))
  def close() = client.close
}


object Elastic {
   import com.sksamuel.elastic4s.http.ElasticDsl._ //Elastic domain Specific Language
   def getType[T: TypeTag](obj: T) = typeOf[T]

   def getAllCountryNameAndCode(countryNumber: Int): Future[Array[(String, String)]] = {
      var countriesQueryFut = ElasticClient.client.execute {
         search("countries" / "countries").matchAllQuery().size(countryNumber)
      }

      countriesQueryFut.map(countriesQuery => countriesQuery match {
         case Left(failure) => Array()
         case Right(countries) => {
            countries.result.hits.hits.map(country => {
               val countryMap = country.sourceAsMap
               (countryMap("name").toString, countryMap("code").toString)
            })
         }
      })
   }

   def getSurfacesByCountryCode(code: String): Future[List[String]] = {
      val agg = termsAggregation("Aggreg").field("surface.keyword")

      var airportsByCountry = ElasticClient.client.execute {
         search("airports" / "airports").matchQuery("iso_country", code)
      }

      airportsByCountry.flatMap {
         case Left(failure) => Future{List()}
         case Right(airports) => {
            val res = airports.result.hits.hits.map(airport => {
               val airportMap = airport.sourceAsMap
               val runwaysByAirport = ElasticClient.client.execute {
                  search("runways" / "runways")
                    .matchQuery("airport_ident", airportMap("ident"))
                    .aggregations(agg)
               }

               runwaysByAirport.map {
                  case Left(failure) => List()
                  case Right(runways) => {
                     runways.result
                            .aggregations
                            .data("Aggreg")
                            .asInstanceOf[Map[String, List[Map[String, String]]]]("buckets")
                            .map(surface => surface("key"))
                  }
               }
            })
            Future.sequence(res.toList).map(_.flatten)
         }
      }
   }

   def reportRunways(countryNumber: Int): Future[String] = {
      println("Surfaces by country:")

      val resultFuture = getAllCountryNameAndCode(countryNumber).map {
         case arrayFuture : Array[(String, String)] => arrayFuture.map {
            case (name: String, code: String) => {
               getSurfacesByCountryCode(code).map(surfaces =>
                  "\t- " + name + ": " + surfaces
                                          .distinct
                                          .filter(surface => { surface != "" })
                                          .mkString("[", ", ", "]")
               )
            }
         }
      }
      resultFuture.flatMap(result => Future.reduce(result)(_ + '\n' + _))
   }

   def reportAirports() : Future[String] = {
      import com.sksamuel.elastic4s.searches.aggs.TermsOrder
      // "keyword" used to make a text field aggregatable in ElasticSearch

      val agg = termsAggregation("Aggreg").field("iso_country.keyword")
      val agg2 = termsAggregation("Aggreg").field("iso_country.keyword").order(TermsOrder("_count"))

      val bestAirportsEither = ElasticClient.client.execute{
         search("airports"/"airports").aggregations(agg)
      }
      val bestFuture : Future[String] = bestAirportsEither.flatMap {
         case Left(failure) => {
            Future{"Best airports not found " + failure.error}
         }
         case Right(bestAirports) => {
            val startString = Future{"Top 10 countries by airports number:"}
            try {
               val resString = bestAirports.result
                  .aggregations
                  .data("Aggreg")
                  .asInstanceOf[Map[String, List[Map[String, Any]]]]("buckets")
                  .map(countryMap => {
                     val countryname : Future[Option[String]] = getCountryNameByCode(countryMap("key").toString)
                     countryname.map(x => x match {
                        case Some(name) => "\t- " + name + ": " + countryMap("doc_count").toString
                        case None => "\t- " + countryMap("key").toString + ": " + countryMap("doc_count").toString
                     })
                  })
               Future.reduce(List(startString, Future.reduce(resString)(_ + '\n' + _)))(_ + '\n' + _)
            } catch {
               case e: NullPointerException => {
                  Future{e.toString}
               }
            }

         }
      }

      val worstAirportsEither = ElasticClient.client.execute{
         search("airports"/"airports").aggregations(agg2)
      }
      val worstFuture : Future[String] = worstAirportsEither.flatMap {
         case Left(failure) => Future{"Worst airports not found " + failure.error}
         case Right(worstAirports) => {
            val startString = Future{"Worst 10 countries by airports number:"}
            try {
               val resString = worstAirports.result
                  .aggregations
                  .data("Aggreg")
                  .asInstanceOf[Map[String, List[Map[String, Any]]]]("buckets")
                  .map(countryMap => {
                     val country : Map[String, Any] = countryMap.asInstanceOf[Map[String, Any]]
                     val countryname : Future[Option[String]] = getCountryNameByCode(country("key").toString)
                     countryname.map(x => x match {
                        case Some(name) => "\t- " + name + ": " + country("doc_count").toString
                        case None => "\t- " + country("key").toString + ": " + country("doc_count").toString
                     })
                  })
               Future.reduce(List(startString, Future.reduce(resString)(_ + '\n' + _)))(_ + '\n' + _)
            } catch {
               case e: NullPointerException => Future{e.toString}
            }
         }
      }
      Future.reduce(List(bestFuture, worstFuture))(_ + "\n\n" + _)
   }

   def reportTop10MostCommonRunwayLatitude(): Future[String] = {
      val agg = termsAggregation("Aggreg").field("le_ident.keyword")

      val commonLatitude = ElasticClient.client.execute {
         search("runways"/"runways").aggregations(agg)
      }
      commonLatitude.map {
         case Left(failure) => "Most common latitude not found " + failure.error
         case Right(commonLat) => {
            val startString = "\nTop 10 most common runway latitude:"
            try {
               commonLat.result
                        .aggregations
                        .data("Aggreg")
                        .asInstanceOf[Map[String, List[Map[String, Any]]]]("buckets")
                        .map(latitude => {
                           "\t- " + latitude("key").toString + ": " + latitude("doc_count").toString
                        }).foldLeft(startString){
                           (acc,elt) => acc + '\n' + elt
                        }
            } catch {
               case e: NullPointerException => {
                  println(e)
                  ""
               }
            }
         }
      }
   }

   def getCountryNameByCode(countryCode: String) : Future[Option[String]] = {
      val searchCountry = ElasticClient.client.execute{
         search("countries"/"countries").matchQuery("code", countryCode).size(1)
      }
      searchCountry.map({
         case Left(failure) => {
            Utils.printKo("Failed to find country code by name" + failure.error)
            None
         }
         case Right(success) => Some(success.result.hits.hits.head.sourceAsMap("name").toString)
      })
   }

   def searchRunwaysByAirports(airportIdent: String) : Future[String] = {
      val searchRunwaysEither = ElasticClient.client.execute{
         search("runways"/"runways").matchQuery("airport_ident", airportIdent)
      }
      val resRunways = searchRunwaysEither.map(runways => runways match {
         case Left(failure) => "\t\tNo runways found for this airport (ident: " + airportIdent +")"
         case Right(searchRunways) => {
            val resString = "\tFound " + Console.BLUE + searchRunways.result.hits.hits.length + Console.WHITE +" available runways:"
            searchRunways.result.hits.hits.map(hit => {
               val hitMap = hit.sourceAsMap
               "\t\tid: " + hitMap("id") + ", length: " + hitMap("length_ft") +
                  " ft., width: " + hitMap("width_ft") + " ft., surface: " + hitMap("surface")+ "\n"
            }).foldLeft(resString + "\n"){(acc, elt) => {
               acc + elt
            }}
         }
      })
      resRunways
   }

   def searchAirportsByCountry(countryCode: String, first : Boolean = true, nb : Int = 10): Future[(String, String)] = {
      val searchAirportsEither = ElasticClient.client.execute{ // FIXME: Use searchScroll
         search("airports"/"airports").matchQuery("iso_country", countryCode).limit(nb)
      }

      searchAirportsEither.flatMap(airportsEither => airportsEither match {
         case Left(failure) => {
            Utils.printKo("No airports found for " + countryCode + "\n")
            Future{("","")}
         }
         case Right(searchAirports) => {
            // var x = getType(searchAirports.hits)
            // println(x.decls.mkString("\n"))
            val airportsFuture = searchAirports.result.hits.hits.map(hit => {
               val hitMap = hit.sourceAsMap
               // (add a wrapper around the resulting string)
               searchRunwaysByAirports(hitMap("ident").toString).map(strRunways => {
                  "\t" + hitMap("name") + ": " + hitMap("type") + " (" + hitMap("municipality") +
                   ", ident: " + hitMap("ident") +")" + "\n"+ strRunways+ "\n"
               })
            })

            // Wrap the mapping in a single future.
            Future.reduce(airportsFuture)(_ + '\n' + _).map(airportsString => {
               if (nb < searchAirports.result.hits.total && first) {
                  (airportsString.concat("Printed a sample of " + Console.BLUE + airportsFuture.length + Console.WHITE
                     + "/" + Console.BLUE + searchAirports.result.hits.total + Console.WHITE + " airports\n"
                     + "How many airports do you want to see?"), countryCode)
               }
               else
                  (airportsString, countryCode)
            })
         }
      })
   }

  // With keyName == "Code" || "Name"
  def searchCountry(keyName: String, countryName: String) : Future[Option[Map[String, AnyRef]]] = {
      // FIXME: Use CompletionSuggestions
      val mysugg = termSuggestion("TermSugg").on(keyName).text(countryName)
      //lazy val mysugg2 = completionSuggestion("CompletionSugg").field(keyName).text(countryName)
      val searchCountryEither = ElasticClient.client.execute{
         search("countries"/"countries").matchQuery(keyName, countryName)
                                        .suggestions(mysugg)
      }

    searchCountryEither.map(countryEither => countryEither match {
      case Left(failure) => {
        Utils.printKo("Country " + countryName + " was not found")
        None
      }
      case Right(searchCountry) => {
         if (searchCountry.result.isEmpty) {
            Utils.printKo("Country " + countryName + " was not found")
            try {
               searchCountry.result.termSuggestion("TermSugg").foreach(termsuggested => {
                  if (termsuggested._2.options.length > 0)
                     println("Maybe you meant:")
                  termsuggested._2.options.foreach(term => println("\t" + term.text))
               })
            } catch {
               case e: NullPointerException => println("Nothing to suggest")
            }
            None
         }
         else {
            if (keyName.equals("name"))
               Utils.printOk("Found country " + countryName + " in ElasticSearch")
            else
               getCountryNameByCode(countryName).foreach(countryNameEither => countryNameEither match {
                  case Some(countryName) => Utils.printOk("Found country " + countryName + " in ElasticSearch")
                  case None => ()

               })


            Some(searchCountry.result.hits.hits.head.sourceAsMap)
         }
      }
   })
  }
}


object csvToElastic {
  import com.sksamuel.elastic4s.http.ElasticDsl._ //Elastic domain Specific Language

  private def toJsonRec(Keys: List[String], Values: List[String]): String = (Keys, Values) match {
    case (a::Nil, b::Nil) => "\"" + a + "\":\"" + b.replaceAllLiterally("\"", "\\\"") + "\""
    case (a::tail1, b::tail2) => "\"" + a + "\":\"" + b.replaceAllLiterally("\"", "\\\"") +
      "\"," + toJsonRec(tail1, tail2)
    case (Nil, Nil) => ""
    case (a::Nil,Nil) => "\"" + a + "\":\"\""
    case (a::tail,Nil) => "\"" + a + "\":\"\"," + toJsonRec(tail, Nil) // Some keys have empty values
    case (Nil,x) => {
      println(x)
      throw new IllegalArgumentException // Some values don't have keys?
    }
  }

  // Header function for recursion, returns (id, json)
  def toJsonSimple(Keys: List[String], Values: List[String]): (String, String) =
    (Values(0), "{" + toJsonRec(Keys, Values) + "}\n")

   def createESIndexIfExists(name: String) : Future[Boolean] = {
      ElasticClient.client.execute{indexExists(name)}.map(existsResponse =>
         if (existsResponse.isLeft) {
            ElasticClient.client.execute{createIndex(name)}
            true
         }
         else {
            !existsResponse.right.get.result.exists
         }
      )
   }

   def csvToElasticGrouped(csv: Csv) = {
      val cols = csv.getCols

      try {
         //Create a list of Json
         val listJsonData = csv.getData.map(line => toJsonSimple(cols, line))
         println("Importing " + csv.getName + " " + listJsonData.length + " lines...")

         // Split the list of Json in 6, since transmission of big chunks is expensive in ElasticSearch
         listJsonData.grouped(listJsonData.length / 6 + 1).foreach(groupJson => {
            val createdEitherFuture = ElasticClient.client.execute{
               bulk ( // Bulk allow me to upload multiple json
                  groupJson.map(json => indexInto(csv.getName/csv.getName).doc(json._2) id json._1)
               ) // Map a json and its id
            }

            createdEitherFuture.map(createdEither => {
               if (createdEither.isLeft) {
                  Utils.printKo("---- Error importing " + csv.getName + " to ElasticSearch ----")
               }
               else
                  Utils.printOk("---- Imported " + csv.getName + " to ElasticSearch (" + groupJson.length
                     + " lines in "  + createdEither.right.get.result.took + " ms.)----")
            })
         })
      /* FIXME: putMapping to update index to add completionSuggestion
      ElasticClient.client.putMapping(csv.getName,csv.getName){
            "suggestionField" as
      }*/
      } catch {
         case e: IllegalArgumentException => {
            Utils.printKo("----- JSON Conversion error for " + csv.getName + " -----")
         }
         case e: Exception => {
            Utils.printKo("---- Exception importing " + csv.getName + " to ElasticSearch ----")
            println(e)
         }
      }
   }
}
