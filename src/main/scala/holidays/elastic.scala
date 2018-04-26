package holidays
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.ElasticImplicits


object csvToElastic {
     import com.sksamuel.elastic4s.http.ElasticDsl._
     lazy val client = HttpClient(ElasticsearchClientUri("localhost", 9200))
     val nameIndex = "holidays"

     private def toJsonRec(Keys: List[String], Values: List[String]): String = (Keys, Values) match {
       case (a::Nil, b::Nil) => "\"" + a + "\":\"" + b.replaceAllLiterally("\"", "\\\"") + "\""
       case (a::tail1, b::tail2) => "\"" + a + "\":\"" + b.replaceAllLiterally("\"", "\\\"") + "\"," + toJsonRec(tail1, tail2)
       case (Nil, Nil) => ""
       case (a::Nil,Nil) => "\"" + a + "\":\"\""
       case (a::tail,Nil) => "\"" + a + "\":\"\"," + toJsonRec(tail, Nil) // Some keys have empty values
       case (_,_) => throw new IllegalArgumentException // Some values don't have keys?
     }

     // Header function for recursion
     def toJsonSimple(Keys: List[String], Values: List[String]): String =
          "{" + toJsonRec(Keys, Values) + "}\n"

     final def createESIndexIfExists(name: String) = {
       val existsResponse = client.execute{indexExists(name)}.await
       if (existsResponse.exists)
          client.execute{createIndex(name)}
     }
/*
     def searchByCountries(countryName: String) = {
       try {
         // FIXME: Use ES Suggestions
         val searchCountry = client.execute{
             search in "holidays"->"countries" termQuery("name", countryName)
         }
         if (searchCountry.isEmpty) {
            printKo("Country " + countryName + " not found")
         }
         else {

         }

       } catch {
         case e: Exception => {

         }
       }
     }*/

      def csvToElastic(csv: Csv) = {
         val cols = csv.getCols

         try {
            val listJsonData = csv.getData.map(line => toJsonSimple(cols, line))


            createESIndexIfExists(nameIndex)
            println("Importing " + csv.getName + " " + listJsonData.length + " lines")
            listJsonData.grouped(15000).foreach(splittedList => {
                  val created = client.execute{
                     bulk (
                        splittedList.map(json => indexInto("holidays"/csv.getName).doc(json) id json(1))
                     )
                  }.await
                  if (created.errors) {
                     Utils.printKo("---- Error importing " + csv.getName + " to ElasticSearch " + created.took + "ms. ----")
                     println(created.items.last)
                     println(created.items.last)
                  }
                  else {
                     Utils.printOk("---- Imported " + csv.getName + " to ElasticSearch ( in "  + created.took + " ms.)----")
                  }
               }
            )

       } catch {
           case e: IllegalArgumentException => {
             Utils.printKo("----- JSON Conversion error for " + csv.getName + " -----")
           }
           case e: Exception => {
               Utils.printKo("---- Exception importing " + csv.getName + " to ElasticSearch ----")
               println(e)
           }
       }

       /*val resp = client.execute {
         search("holidays") query "24/04/2018"
       }.await*/
       //println(resp.hits)
       // resp match {
       //      case Left(failure) => println("We failed " + failure.error)
       //      case Right(results) => println(results.result.hits)
       // }

       client.close()
     }
}
