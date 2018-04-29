import org.scalatest.FunSuite
   import com.sksamuel.elastic4s.http._
   import com.sksamuel.elastic4s.ElasticsearchClientUri
   import com.sksamuel.elastic4s.ElasticImplicits


class testCsv extends FunSuite {
   import com.sksamuel.elastic4s.http.ElasticDsl._

   def cmpList(l1: List[String], l2: List[String]) : Unit = (l1, l2) match {
      case (a::b, c::d) if (a == b) => cmpList(b,d)
      case (a::b, c::d) => print(a + " is different of " + c)
      case (Nil, Nil) =>
   }


   val brazil = "302791,\"BR\",\"Brazil\",\"SA\",\"http://en.wikipedia.org/wiki/Brazil\",\"Brasil, Brasilian\""
   val longAirport = "19485,\"KCFS\",\"small_airport\",\"Tuscola Area Airport\",43.458801269531,-83.445503234863," +
                     "701,\"NA\",\"US\",\"US-MI\",\"Caro\",\"no\",\"KCFS\",\"TZC\",\"CFS\","+
                     "\"http://www.michigan.gov/aero/0,4533,7-145-61367-277436--,00.html\","+
                     "\"http://en.wikipedia.org/wiki/Tuscola_Area_Airport\",\"78D\""
   val helmand_airp = "35359,\"AF04\",\"small_airport\",\"Helmand Airport\",30.33009910583496,61.89690017700195,,"+
                      "\"AS\",\"AF\",\"AF-NIM\",,\"no\",,,,,,"

   val listKeysCountries = List("id", "code", "name", "continent", "wikipedia_link", "keywords")

   val ruskov = "\"44644\",\"RU-0181\",\"heliport\",\"Solntsevo Heliport\",\"55.643001556396484\",\"37.387001037597656\",,"+
   "\"EU\",\"RU\",\"RU-MOW\",\"Moscow\",\"no\",\"UUWS\",,,\"http://www.aovzlet.ru/ENG/index.html\",,\"Vzlyot Heliport, Vzlet Heliport, Вертодром Солнцево, Вертодром НПО \"\"Взлёт\"\", Вертодром НПО \"\"Взлет\"\"\""

   test("Parse_csv.toFloat") {
      assert(holidays.Parse_csv.toFloat("-8.5") === Some(-8.5))
   }
   test("Parse_csv.toFloat2") {
      assert(holidays.Parse_csv.toFloat("") === None)
   }
   test("Parse_csv.toFloat3") {
      assert(holidays.Parse_csv.toFloat("4") === Some(4))
   }
   test("Parse_csv.toFloatNeg") {
      assert(holidays.Parse_csv.toFloat("-4445") === Some(-4445))
   }

   test("Parse_csv.combineListOnCommas.Brazil") {
      val res = holidays.Parse_csv.combineListOnCommas(brazil.split(",").toList)
      val list = List("302791", "BR", "Brazil", "SA",
         "http://en.wikipedia.org/wiki/Brazil", "Brasil, Brasilian")
      assert(res === list)
   }

   test("Parse_csv.combineListOnCommas.Helmand") {
      val list = List("35359","AF04","small_airport","Helmand Airport",
         "30.33009910583496","61.89690017700195", "","AS","AF","AF-NIM", "","no","","","","","","")
      val res = holidays.Parse_csv.combineListOnCommas(helmand_airp.split(",").toList).padTo(list.length, "")
      assert(res === list)
   }

   test("Parse_csv.combineListOnCommas.longAirport") {
      val res = holidays.Parse_csv.combineListOnCommas(longAirport.split(",").toList)
      val list = List("19485","KCFS","small_airport","Tuscola Area Airport",
         "43.458801269531","-83.445503234863","701","NA","US","US-MI",
         "Caro","no","KCFS","TZC","CFS",
         "http://www.michigan.gov/aero/0,4533,7-145-61367-277436--,00.html",
         "http://en.wikipedia.org/wiki/Tuscola_Area_Airport","78D")
      /*println(res)
      println(list)*/
      assert(res === list)
   }

   test("Parse_csv.combineListOnCommas.Ruskov") {
      val res = holidays.Parse_csv.combineListOnCommas(ruskov.split(",").toList)
      val list = List("44644","RU-0181","heliport","Solntsevo Heliport",
         "55.643001556396484","37.387001037597656","","EU","RU","RU-MOW","Moscow","no","UUWS","","",
         "http://www.aovzlet.ru/ENG/index.html",
         "","Vzlyot Heliport, Vzlet Heliport, Вертодром Солнцево, Вертодром НПО \"\"Взлёт\"\", Вертодром НПО \"\"Взлет\"\"")
      println(res)
      println(list)
      cmpList(res, list)
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
      assert(searchCountry.isRight === true)
      assert(searchCountry.right.get.result.isEmpty === false)
      assert(searchCountry.right.get.result.hits.hits.head.sourceAsMap("name") === "Brazil")
   }
}
