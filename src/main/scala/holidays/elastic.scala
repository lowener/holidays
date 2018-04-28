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

   // With keyName == Code || Name
   def searchByCountry(keyName: String, countryName: String) : Option[Map[String, AnyRef]] = {
      try {
         // FIXME: Use ES Suggestions
         val searchCountry = ElasticClient.client.execute{
             search("countries"/"countries").matchQuery(keyName, countryName)
         }.await
         if (searchCountry.isEmpty) {
            Utils.printKo("Country " + countryName + " not found")
            None
         }
         else {
            Utils.printOk("Found country " + countryName + " in ElasticSearch")
            Some(searchCountry.hits.hits.head.sourceAsMap)
         }

      } catch {
         case e: Exception => None
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
      case (Nil,x) =>
         throw new IllegalArgumentException // Some values don't have keys?
   }

   // Header function for recursion, returns (id, json)
   def toJsonSimple(Keys: List[String], Values: List[String]): (String, String) =
       (Values(0), "{" + toJsonRec(Keys, Values) + "}\n")

   def createESIndexIfExists(name: String) : Boolean = {
      val existsResponse = ElasticClient.client.execute{indexExists(name)}.await
         if (existsResponse.exists) {
          ElasticClient.client.execute{createIndex(name)}
          true
      }
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
