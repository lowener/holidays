import org.scalatest.FunSuite
   import com.sksamuel.elastic4s.http._
   import com.sksamuel.elastic4s.ElasticsearchClientUri
   import com.sksamuel.elastic4s.ElasticImplicits


class testCsv extends FunSuite {
   import com.sksamuel.elastic4s.http.ElasticDsl._
   val brazil = "302791,\"BR\",\"Brazil\",\"SA\",\"http://en.wikipedia.org/wiki/Brazil\",\"Brasil, Brasilian\""
   val listKeysCountries = List("id", "code", "name", "continent", "wikipedia_link", "keywords")

   test("Parse_csv.toInt") {
      assert(holidays.Parse_csv.toInt("") === None)
   }
   test("Parse_csv.toInt2") {
      assert(holidays.Parse_csv.toInt("4") === Some(4))
   }
   test("Parse_csv.toIntNeg") {
      assert(holidays.Parse_csv.toInt("-4445") === Some(-4445))
   }

   test("Parse_csv.combineListOnCommas.Brazil") {
      val res = holidays.Parse_csv.combineListOnCommas(brazil.split(",").toList)
      val list = List("302791", "BR", "Brazil", "SA",
         "http://en.wikipedia.org/wiki/Brazil", "Brasil,Brasilian")
      assert(res === list)
   }

   test("csvToElastic.toJsonSimple") {
      val listKeys = listKeysCountries
      val listValues = List("302791", "BR", "Brazil", "SA",
         "http://en.wikipedia.org/wiki/Brazil", "Brasil,Brasilian")

      val res = holidays.csvToElastic.toJsonSimple(listKeys, listValues)
      assert(res._1 === "302791")
      assert(res._2 === "{\"id\":\"302791\",\"code\":\"BR\",\"name\":\"Brazil\",\"continent\":\"SA\",\"wikipedia_link\":\"http://en.wikipedia.org/wiki/Brazil\",\"keywords\":\"Brasil,Brasilian\"}\n")
   }


   test("csvToElasticGrouped.Brazil") {

      val listValues = List("302791", "BR", "Brazil", "SA",
         "http://en.wikipedia.org/wiki/Brazil", "Brasil,Brasilian")
      val csv = new holidays.Csv("testholidays", listKeysCountries, List(listValues))
      holidays.csvToElastic.csvToElasticGrouped(csv)
      Thread.sleep(400) // Wait for ElasticSearch to update its database. 

      val searchCountry = holidays.ElasticClient.client.execute{
          search("testholidays/testholidays").matchQuery("name", "Brazil")
      }.await
      println(searchCountry)
      assert(searchCountry.isEmpty === false)
      assert(searchCountry.hits.hits.head.sourceAsMap("name") === "Brazil")
   }
}
