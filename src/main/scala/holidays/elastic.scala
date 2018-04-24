package holidays
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.ElasticImplicits


object csvToElastic {
     import com.sksamuel.elastic4s.http.ElasticDsl._

     private def toJsonRec(Keys: List[String], Values: List[String]): String = (Keys, Values) match {
       case (a::Nil, b::Nil) => "\t\"" + a + "\"\t:\t\"" + b + "\"\n}"
       case (a::tail1, b::tail2) => "\t\"" + a + "\"\t:\t\"" + b + "\",\n" + toJsonRec(tail1, tail2)
       case (Nil, Nil) => " }"
       case (_,_) => throw new IllegalArgumentException
     }

     // Header function for recursion
     def toJsonSimple(Keys: List[String], Values: List[String]): String =
       "{\n" + toJsonRec(Keys, Values)

     def csvToElastic(csv: Csv) = {
       val client = HttpClient(ElasticsearchClientUri("localhost", 9200))

       val data = csv.getData
       val cols = csv.getCols

       client.execute{
         createIndex("csv")
       }

       val listJsonData = data.map(line => toJsonSimple(cols, line))

       val create = client.execute{
         indexInto("csv"/csv.getName).source("{ \"id\" : \"1\", \"date\": \"24/04/2018\"}")
       }.await

       println("---- Search Results ----")
       println(create.toString)

       val resp = client.execute {
         search("airports") query "24/04/2018"
       }.await

       println("---- Search Results ----")
       println(resp.hits)
       // resp match {
       //      case Left(failure) => println("We failed " + failure.error)
       //      case Right(results) => println(results.result.hits)
       // }

       client.close()
     }
}
