package holidays
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http._
import scala.reflect.runtime.universe._


object ElasticClient{
  lazy val client = HttpClient(ElasticsearchClientUri("localhost", 9200))
  def close() = client.close
}


object Elastic {
  import com.sksamuel.elastic4s.http.ElasticDsl._ //Elastic domain Specific Language
  def getType[T: TypeTag](obj: T) = typeOf[T]

  def getAllCountryNameAndCode(countryNumber: Int): Array[(String, String)] = {
    var countriesQuery = ElasticClient.client.execute {
      search("countries" / "countries").matchAllQuery().size(countryNumber)
    }.await

    countriesQuery match {
      case Left(failure) => Array()
      case Right(countries) => {
        countries.result.hits.hits.map(country => {
          val countryMap = country.sourceAsMap
          (countryMap("name").toString, countryMap("code").toString)
        })
      }
    }
  }

  def getSurfacesByCountryCode(code: String): Array[String] = {
    val agg = termsAggregation("Aggreg").field("surface.keyword")

    var airportsByCountry = ElasticClient.client.execute {
      search("airports" / "airports").matchQuery("iso_country", code)
    }.await

    airportsByCountry match {
      case Left(failure) => Array()
      case Right(airports) => {
        airports.result.hits.hits.flatMap(airport => {
          val airportMap = airport.sourceAsMap
          val runwaysByAirport = ElasticClient.client.execute {
            search("runways" / "runways").matchQuery("airport_ref", airportMap("id")).aggregations(agg)
          }.await

          runwaysByAirport match {
            case Left(failure) => List()
            case Right(runways) => {
              runways.result
                .aggregations
                .data("Aggreg")
                .asInstanceOf[Map[String, List[Map[String, String]]]]("buckets")
                .map(surface => surface("key"))
            }
          }
        }).distinct.filter(surface => { surface != "" })
      }
    }
  }

  def reportRunways(countryNumber: Int): Unit = {
    println("Surfaces by country:")

    getAllCountryNameAndCode(countryNumber).foreach {
      case (name: String, code: String) => {
            val countrySurfaces = getSurfacesByCountryCode(code)
            println("\t- " + name + ": " + countrySurfaces.mkString("[", ", ", "]"))
      }
    }
  }

  def reportTop10MostCommonRunwayLatitude(): Unit  = {
    val agg = termsAggregation("Aggreg").field("le_ident.keyword")

    val commonLatitude = ElasticClient.client.execute {
      search("runways"/"runways").aggregations(agg)
    }.await
    commonLatitude match {
      case Left(failure) => Utils.printKo("Most common latitude not found " + failure.error)
      case Right(commonLat) => {
        println("Top 10 most common runway latitude:")
        try {
          commonLat.result
            .aggregations
            .data("Aggreg")
            .asInstanceOf[Map[String, List[Map[String, Any]]]]("buckets")
            .foreach(latitude => {
              println("\t- " + latitude("key").toString + ": " + latitude("doc_count").toString)
            })
        } catch {
          case e: NullPointerException => println(e)
        }
      }
    }
  }

  def reportAirports(): Unit = {
    import com.sksamuel.elastic4s.searches.aggs.TermsOrder
    // "keyword" used to make a text field aggregatable in ElasticSearch

    val agg = termsAggregation("Aggreg").field("iso_country.keyword")
    val agg2 = termsAggregation("Aggreg").field("iso_country.keyword").order(TermsOrder("_count"))

    val bestAirportsEither = ElasticClient.client.execute{
      search("airports"/"airports").aggregations(agg)
    }.await
    bestAirportsEither match {
      case Left(failure) => Utils.printKo("Best airports not found " + failure.error)
      case Right(bestAirports) => {
        println("Top 10 countries by airports number:")
        try {
          bestAirports.result
            .aggregations
            .data("Aggreg")
            .asInstanceOf[Map[String, List[Map[String, Any]]]]("buckets")
            .foreach(countryMap => {
              val country : Map[String, Any] = countryMap
              val x = getCountryNameByCode(country("key").toString)
              x match {
                case Some(name) => println("\t- " + name + ": " + country("doc_count").toString)
                case None => println("\t- " + country("key").toString + ": " + country("doc_count").toString)
              }
            })
        } catch {
          case e: NullPointerException => println(e)
        }
      }
    }


    val worstAirportsEither = ElasticClient.client.execute{
      search("airports"/"airports").aggregations(agg2)
    }.await
    worstAirportsEither match {
      case Left(failure) => Utils.printKo("Worst airports not found " + failure.error)
      case Right(worstAirports) => {
        println("Worst 10 countries by airports number:")
        try {
          worstAirports.result
            .aggregations
            .data("Aggreg")
            .asInstanceOf[Map[String, List[Map[String, Any]]]]("buckets")
            .foreach(countryMap => {
              val country : Map[String, Any] = countryMap.asInstanceOf[Map[String, Any]]
              val x = getCountryNameByCode(country("key").toString)
              x match {
                case Some(name) => println("\t- " + name + ": " + country("doc_count").toString)
                case None => println("\t- " + country("key").toString + ": " + country("doc_count").toString)
              }
            })
        } catch {
          case e: NullPointerException => println(e)
        }

      }
    }
  }

  def getCountryNameByCode(countryCode: String)  = {
    val searchCountry = ElasticClient.client.execute{
      search("countries"/"countries").matchQuery("code", countryCode).size(1)
    }.await
    searchCountry match {
      case Left(failure) => {
        Utils.printKo("Failed to find country code by name" + failure.error)
        None
      }
      case Right(success) => Some(success.result.hits.hits.head.sourceAsMap("name").toString)
    }

  }

  def searchRunwaysByAirports(airportIdent: String) {
    val searchRunwaysEither = ElasticClient.client.execute{
      search("runways"/"runways").matchQuery("airport_ident", airportIdent)
    }.await
    searchRunwaysEither  match {
      case Left(failure) => println("\t\tNo runways found for this airport (ident: " + airportIdent +")")
      case Right(searchRunways) => {
        println("\tFound " + Console.BLUE + searchRunways.result.hits.hits.length + Console.WHITE +" available runways:")
        searchRunways.result.hits.hits.map(hit => {
          val hitMap = hit.sourceAsMap
          println("\t\tid: " + hitMap("id") + ", length: " + hitMap("length_ft") +
            " ft., width: " + hitMap("width_ft") + " ft., surface: " + hitMap("surface"))
        })
      }
    }
  }

  def searchAirportsByCountry(countryCode: String, first : Boolean = true, nb : Int = 10): Unit = {
    val searchAirportsEither = ElasticClient.client.execute{ // FIXME: Use searchScroll
      search("airports"/"airports").matchQuery("iso_country", countryCode).limit(nb)
    }.await

    match {
      case Left(failure) => Utils.printKo("No airports found for " + countryCode)
      case Right(searchAirports) => {
        // var x = getType(searchAirports.hits)
        // println(x.decls.mkString("\n"))
        searchAirports.result.hits.hits.map(hit => {
          val hitMap = hit.sourceAsMap
          println("\t" + hitMap("name") + ": " + hitMap("type") + " (" + hitMap("municipality")
            + ", ident: " + hitMap("ident") +")")
          searchRunwaysByAirports(hitMap("ident").toString)
        })

        if (nb < searchAirports.result.hits.total && first)
          println("Printed a sample of " + Console.BLUE + searchAirports.result.hits.size + Console.WHITE
            + "/" + Console.BLUE + searchAirports.result.hits.total + Console.WHITE + " airports")
        if (nb < searchAirports.result.hits.total && first) {
          println("How many airports do you want to see?")
          try {
            val i = readInt
            if (i > 0)
              searchAirportsByCountry(countryCode, false, i)
          } catch {
            case e: NumberFormatException => println("Wrong input")
          }

        }
      }
    }
  }

  // With keyName == Code || Name
  def searchCountry(keyName: String, countryName: String) : Option[Map[String, AnyRef]] = {
    // FIXME: Use CompletionSuggestions

    val mysugg = termSuggestion("TermSugg").on(keyName).text(countryName)
    //lazy val mysugg2 = completionSuggestion("CompletionSugg").field(keyName).text(countryName)
    val searchCountryEither = ElasticClient.client.execute{
      search("countries"/"countries").matchQuery(keyName, countryName)
        .suggestions(mysugg)
    }.await

    searchCountryEither match {
      case Left(failure) => {
        Utils.printKo("Country " + countryName + " was not found")
        try { // Suggestion
          None
          /*val completionCountry = ElasticClient.client.execute{
              search("countries").suggestions(mysugg2)
          }.await*/
        } catch { // Try catch is mandatory here, the library offers no way to check if "TermSugg" exists...
          case e: NullPointerException => {
            println("No suggestions available")
            None
          }
        }
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
            Utils.printOk("Found country " + getCountryNameByCode(countryName).getOrElse(countryName) + " in ElasticSearch")
          Some(searchCountry.result.hits.hits.head.sourceAsMap)
        }
      }
    }
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

  def createESIndexIfExists(name: String) : Boolean = {
    val existsResponse = ElasticClient.client.execute{indexExists(name)}.await
    if (existsResponse.isLeft) {
      ElasticClient.client.execute{createIndex(name)}
      true
    }
    else {
      !existsResponse.right.get.result.exists
    }
  }

  def csvToElasticGrouped(csv: Csv) = {
    val cols = csv.getCols

    try {
      //Create a list of Json
      val listJsonData = csv.getData.map(line => toJsonSimple(cols, line))

      createESIndexIfExists(csv.getName)
      println("Importing " + csv.getName + " " + listJsonData.length + " lines...")

      // Split the list of Json in 4, to not split the transmission to ElasticSearch
      listJsonData.grouped(listJsonData.length / 4 + 1).foreach(groupJson => {
        val createdEither = ElasticClient.client.execute{
          bulk ( // Bulk allow me to upload multiple json
            groupJson.map(json => indexInto(csv.getName/csv.getName).doc(json._2) id json._1)
          ) // Map a json and its id
        }.await
        if (createdEither.isLeft) {
          Utils.printKo("---- Error importing " + csv.getName + " to ElasticSearch ----")
        }
        else
          Utils.printOk("---- Imported " + csv.getName + " to ElasticSearch (" + groupJson.length
            + " lines in "  + createdEither.right.get.result.took + " ms.)----")
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
