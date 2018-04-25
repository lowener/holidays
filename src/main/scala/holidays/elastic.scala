package holidays
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.ElasticImplicits


object csvToElastic {
     import com.sksamuel.elastic4s.http.ElasticDsl._
     lazy val client = HttpClient(ElasticsearchClientUri("localhost", 9200))
     val nameIndex = "holidays"

     private def toJsonRec(Keys: List[String], Values: List[String]): String = (Keys, Values) match {
       case (a::Nil, b::Nil) => "\"" + a + "\":\"" + b + "\"}\n"
       case (a::tail1, b::tail2) => "\"" + a + "\":\"" + b + "\"," + toJsonRec(tail1, tail2)
       case (Nil, Nil) => " }\n"
       case (_,_) => throw new IllegalArgumentException
     }

     // Header function for recursion
     def toJsonSimple(Keys: List[String], Values: List[String]): String =
       "{" + toJsonRec(Keys, Values)

     def createESIndexIfExists(name: String) = {
       val existsResponse = client.execute{indexExists(name)}.await
       if (existsResponse.exists)
          client.execute{createIndex(name)}
     }

     def csvToElastic(csv: Csv) = {
       val cols = csv.getCols
       val listJsonData = csv.getData.map(line => toJsonSimple(cols, line))

       createESIndexIfExists(nameIndex)
       println(listJsonData(0))
       // val bulkRequest = BulkProcessorBuilder.build(client)
       //println(List("{\"iso\":\"US\", \"id\": \"2\"}", "{\"iso\":\"FR\", \"id\": \"3\"}"))
       try {
         val create = client.execute{
           bulk (
             /*List("{\"iso\":\"US\", \"id\": \"2\"}", "{\"iso\":\"FR\", \"id\": \"3\"}")
               .map(json => indexInto("holidays"/csv.getName).source(json))*/
             listJsonData.map(json => indexInto("holidays"/csv.getName).doc(json))
           )
         }.await


         println("---- Create Results ----")
         println(create.toString)
       } catch {
         case e: Exception => println(e)
       }



       /*val resp = client.execute {
         search("holidays") query "24/04/2018"
       }.await*/

       println("---- Search Results ----")
       //println(resp.hits)
       // resp match {
       //      case Left(failure) => println("We failed " + failure.error)
       //      case Right(results) => println(results.result.hits)
       // }

       client.close()
     }
}
