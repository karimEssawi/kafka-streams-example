//package de.joyn
//
//import java.util.Properties
//
//import com.moleike.kafka.streams.avro.generic.Serdes.Config
//import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
//import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
//import org.apache.avro.generic.GenericRecord
//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.kafka.streams.KeyValue
////import org.apache.kafka.common.serialization.Serdes
//import org.apache.kafka.streams.kstream.{KStream, KTable}
//import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
//import org.apache.kafka.streams.scala.Serdes
//import org.apache.kafka.streams.scala.ImplicitConversions._
//import org.apache.kafka.streams.scala._
//import org.apache.kafka.streams.scala.kstream._
//
//
//case class PageView(viewtime: Long, userid: String, pageid: String)
//
//object Example extends App {
//  implicit val conf: Config = Map(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://localhost:8081")
//
//  val config: Properties = {
//    val p = new Properties()
//    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "pageview-gender-example")
//    p.put(StreamsConfig.CLIENT_ID_CONFIG, "pageview-gender-example-client")
//    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
////    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
////    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
//
////    import org.apache.kafka.streams.StreamsConfig
////    p.put(StreamsConfig.CLIENT_ID_CONFIG, "pageview-region-lambda-example-client")
//
//    p.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")
//
//    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
//    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde])
//    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//
//    p
//  }
//
//
//
//  val builder = new StreamsBuilder()
//
//  val views: KStream[String, PageView] = builder.stream("pageviews")
//  views.map((_, record) => new KeyValue[String, PageView](record.userid, record))
//
//  val userProfiles = builder.table("users")
//  val userGender = userProfiles.mapValues((record: GenericRecord) => record.get("gender").toString)
//
////  val wordCounts: KTable[String, Long] = textLines
////    .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
////    .groupBy((_, word) => word)
////    .count()
////  wordCounts.toStream.to("streams-wordcount-output")
//
//  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
//}
