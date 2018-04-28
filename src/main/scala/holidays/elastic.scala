package holidays
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.ElasticImplicits


object ElasticClient{
   lazy val client = HttpClient(ElasticsearchClientUri("localhost", 9200))
   def close = client.close
}


object Elastic {
   import com.sksamuel.elastic4s.http.ElasticDsl._ //Elastic domain Specific Language

   def searchAirportsbyCountry(countryCode: String) = {
      val searchAirports = ElasticClient.client.execute{
          search("airports"/"airports").matchQuery("iso_country", countryCode)
      }.await

      if (searchAirports.isEmpty) {
         Utils.printKo("No airports found for " + countryCode)
      }
      else {
         println("Found " + Console.BLUE + searchAirports.hits.hits.length + Console.WHITE +" available airports:")
         searchAirports.hits.hits.map(hit => {
            val hitMap = hit.sourceAsMap
            println("\t" + hitMap("name") + ": " + hitMap("type") + " (" + hitMap("municipality") + ")")
         })
      }
   }

   // With keyName == Code || Name
   def searchCountry(keyName: String, countryName: String) : Option[Map[String, AnyRef]] = {
      try {
         // FIXME: Use CompletionSuggestions
         val mysugg = termSuggestion("TermSugg").field(keyName).text(countryName)
         //lazy val mysugg2 = completionSuggestion("CompletionSugg").field(keyName).text(countryName)
         val searchCountry = ElasticClient.client.execute{
             search("countries"/"countries").matchQuery(keyName, countryName).suggestions(mysugg)
         }.await
         if (searchCountry.isEmpty) {
            //println(searchCountry)
            Utils.printKo("Country " + countryName + " was not found")
            try { // Suggestion
               searchCountry.termSuggestion("TermSugg").foreach(termsuggested => {
                  if (termsuggested._2.options.length > 0)
                     println("Maybe you meant:")
                  termsuggested._2.options.foreach(term => println("\t" + term.text))
               })
               /*val completionCountry = ElasticClient.client.execute{
                   search("countries").suggestions(mysugg2)
               }.await*/
            } catch { // Try catch is mandatory here, the library offers no way to check if "TermSugg" exists...
               case e: NullPointerException => {
                  println("No suggestions available")
               }
            }
            None
         }
         else {
            Utils.printOk("Found country " + countryName + " in ElasticSearch")
            Some(searchCountry.hits.hits.head.sourceAsMap)
         }
      } catch {
         case e: Exception => {
            Utils.printKo("Exception: Country " + countryName + " was not found")
            println(e)
            None
         }
      }
   }
}


object csvToElastic {
   import com.sksamuel.elastic4s.http.ElasticDsl._ //Elastic domain Specific Language

   private def toJsonRec(Keys: List[String], Values: List[String]): String = (Keys, Values) match {
      case (a::Nil, b::Nil) => "\"" + a + "\":\"" + b.replaceAllLiterally("\"", "\\\"") + "\""
      case (a::tail1, b::tail2) => "\"" + a + "\":\"" + b.replaceAllLiterally("\"", "\\\"") + "\"," + toJsonRec(tail1, tail2)
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
      if (!existsResponse.exists) {
          ElasticClient.client.execute{createIndex(name)}
          true
      }
      else
         false
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
            val created = ElasticClient.client.execute{
               bulk ( // Bulk allow me to upload multiple json
                  groupJson.map(json => indexInto(csv.getName/csv.getName).doc(json._2) id json._1)
               ) // Map a json and its id
            }.await
            if (created.errors) {
               Utils.printKo("---- Error importing " + csv.getName + " to ElasticSearch " + created.took + "ms. ----")
               println(created.items.last)
            }
            else
               Utils.printOk("---- Imported " + csv.getName + " to ElasticSearch (" + groupJson.length + " lines in "  + created.took + " ms.)----")
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
