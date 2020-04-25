/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package io.confluent.examples.streams;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.InputStream;
import java.time.Duration;
import java.util.*;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxBytes;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;

/**
 * Demonstrates how to perform a join between a KStream and a KTable, i.e. an example of a stateful
 * computation, using the generic Avro binding for serdes in Kafka Streams.  Same as
 * PageViewRegionExample but uses lambda expressions and thus only works on Java 8+.
 * <p>
 * In this example, we join a stream of page views (aka clickstreams) that reads from a topic named
 * "PageViews" with a user profile table that reads from a topic named "UserProfiles" to compute the
 * number of page views per user region.
 * <p>
 * Note: The generic Avro binding is used for serialization/deserialization. This means the
 * appropriate Avro schema files must be provided for each of the "intermediate" Avro classes, i.e.
 * whenever new types of Avro objects (in the form of GenericRecord) are being passed between
 * processing steps.
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start Zookeeper, Kafka, and Confluent Schema Registry. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
 * <p>
 * 2) Create the input/intermediate/output topics used by this example.
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic PageViews \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic UserProfiles \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic PageViewsByRegion \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * }
 * </pre>
 * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be {@code bin/kafka-topics.sh ...}.
 * <p>
 * 3) Start this example application either in your IDE or on the command line.
 * <p>
 * If via the command line please refer to <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>.
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-5.4.1-standalone.jar io.confluent.examples.streams.PageViewRegionLambdaExample
 * }
 * </pre>
 * 4) Write some input data to the source topics (e.g. via {@link PageViewRegionExampleDriver}).
 * The already running example application (step 3) will automatically process this input data and
 * write the results to the output topic.
 * <pre>
 * {@code
 * # Here: Write input data using the example driver. Once the driver has stopped generating data,
 * # you can terminate it via `Ctrl-C`.
 * $ java -cp target/kafka-streams-examples-5.4.1-standalone.jar io.confluent.examples.streams.PageViewRegionExampleDriver
 * }
 * </pre>
 * 5) Inspect the resulting data in the output topic, e.g. via {@code kafka-console-consumer}.
 * <pre>
 * {@code
 * $ bin/kafka-console-consumer --topic PageViewsByRegion --from-beginning \
 *                              --bootstrap-server localhost:9092 \
 *                              --property print.key=true \
 *                              --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 * }
 * </pre>
 * You should see output data similar to:
 * <pre>
 * {@code
 * [africa@1466515140000]  2
 * [asia@1466514900000]  3
 * ...
 * }
 * </pre>
 * Here, the output format is "[REGION@WINDOW_START_TIME] COUNT".
 * <p>
 * 6) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}. If needed,
 * also stop the Confluent Schema Registry ({@code Ctrl-C}), then stop the Kafka broker ({@code Ctrl-C}), and
 * only then stop the ZooKeeper instance ({@code Ctrl-C}).
 */
public class PageViewRegionLambdaExample {

    public static void main(final String[] args) throws Exception {
        String schemaRegistryUrl = "http://localhost:8081";

        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "pageview-region-lambda-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "pageview-region-lambda-example-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the Confluent schema registry instance(s)
        streamsConfiguration.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);

        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        final Serde<GenericRecord> keyAvroSerde = new GenericAvroSerde();
        keyAvroSerde.configure(serdeConfig, true);

        final Serde<GenericRecord> valueAvroSerde = new GenericAvroSerde();
        valueAvroSerde.configure(serdeConfig, false);

        final StreamsBuilder builder = new StreamsBuilder();

        // Create a stream of page view events from the PageViews topic, where the key of
        // a record is assumed to be null and the value an Avro GenericRecord
        // that represents the full details of the page view event. See `pageview.avsc` under
        // `src/main/avro/` for the corresponding Avro schema.
        final KStream<String, GenericRecord> views = builder.stream("pageviews");

        // Create a keyed stream of page view events from the PageViews stream,
        // by extracting the user id (String) from the Avro value
        final KStream<String, GenericRecord> viewsByUser = views
                .map((dummy, record) -> new KeyValue<>(record.get("userid").toString(), record));

        // Create a changelog stream for user profiles from the UserProfiles topic,
        // where the key of a record is assumed to be the user id (String) and its value
        // an Avro GenericRecord.  See `userprofile.avsc` under `src/main/avro/` for the
        // corresponding Avro schema.
        final KTable<String, GenericRecord> userProfiles = builder.table("users");

        // Create a changelog stream as a projection of the value to the region attribute only
        final KTable<String, String> userGender = userProfiles.mapValues(record -> record.get("gender").toString());

        // We must specify the Avro schemas for all intermediate (Avro) classes, if any.
        // In this example, we want to create an intermediate GenericRecord to hold the view region.
        // See `pageviewgender.avsc` under `src/main/avro/`.
        final InputStream pageViewRegionSchema = PageViewRegionLambdaExample.class.getClassLoader().getResourceAsStream("avro/pageviewgender.avsc");
        final Schema schema = new Schema.Parser().parse(pageViewRegionSchema);

        final Comparator<GenericRecord> comparator = (o1, o2) -> (int) ((Long) o2.get("viewtime") - (Long) o1.get("viewtime"));

        final Serde<Windowed<String>> windowedStringSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

        final KTable<Windowed<String>, GenericRecord> totalViewTimeByGender = viewsByUser
                .leftJoin(userGender, (view, gender) -> {
                    final GenericRecord viewGender = new GenericData.Record(schema);
                    viewGender.put("userid", view.get("userid"));
                    viewGender.put("pageid", view.get("pageid"));
                    viewGender.put("viewtime", view.get("viewtime"));
                    viewGender.put("gender", gender);
                    return viewGender;
                })
                // map key to gender and pageId - This is really hacky, Use a Pair instead?
                .map((dummy, viewGender) -> new KeyValue<>(viewGender.get("gender").toString() + viewGender.get("pageid").toString(), viewGender))
                // calculate total viewtimes for each gender-pageId key, using hopping windows of size 1 minute that advance every 10 seconds
                .groupByKey() // no need to specify explicit serdes because the resulting key and value types match our default serde settings
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofSeconds(10)))
//                .count();
                .reduce((base, acc) -> {
                    final GenericRecord viewGender = new GenericData.Record(schema);
                    viewGender.put("userid", base.get("userid") + "|" + acc.get("userid"));
                    viewGender.put("pageid", base.get("pageid"));
                    viewGender.put("viewtime", Long.parseLong(base.get("viewtime").toString()) + Long.parseLong(acc.get("viewtime").toString()));
                    viewGender.put("gender", base.get("gender").toString());
                    return viewGender;
                });

//                .aggregate(
//                        () -> new PriorityQueue<>(comparator),
//                        // the "add" aggregator
//                        (windowedPair, record, queue) -> {
//                            queue.add(record.);
//                            return queue;
//                        },
//                        // the "remove" aggregator
//                        (windowedIndustry, record, queue) -> {
//                            queue.remove(record);
//                            return queue;
//                        },
//
//                        Materialized.with(windowedStringSerde, new PriorityQueueSerde<>(comparator, valueAvroSerde))
//                );

        // Note: The following operations would NOT be needed for the actual pageview-by-region
        // computation, which would normally stop at `count` above.  We use the operations
        // below only to "massage" the output data so it is easier to inspect on the console via
        // kafka-console-consumer.
//        final KStream<String, GenericRecord> viewsByRegionForConsole = totalViewTimeByGender
        final InputStream pageViewGenderSchema = PageViewRegionLambdaExample.class.getClassLoader().getResourceAsStream("avro/pageviewgenderuser.avsc");
        final Schema finalSchema = new Schema.Parser().parse(pageViewGenderSchema);

        final KStream<String, GenericRecord> viewsByRegionForConsole = totalViewTimeByGender
                // get rid of windows (and the underlying KTable) by transforming the KTable to a KStream
                // and by also converting the record key from type `Windowed<String>` (which
                // kafka-console-consumer can't print to console out-of-the-box) to `String`
//                .suppress(untilTimeLimit(Duration.ofMinutes(1), null))
                .toStream((windowedRegion, count) -> windowedRegion.toString())
                .mapValues(v -> {
                    HashSet<String> users = new HashSet<>(Arrays.asList(v.get("userid").toString().split("\\|")));
                    final GenericRecord viewGender = new GenericData.Record(finalSchema);
                    viewGender.put("gender", v.get("gender"));
                    viewGender.put("pageid", v.get("pageid").toString());
                    viewGender.put("totalviewtime", v.get("viewtime"));
                    viewGender.put("usercount", (long) users.size());
                    return viewGender;
                });

//        viewsByRegionForConsole.to("PageViewsByGender", Produced.with(stringSerde, valueAvroSerde));
        viewsByRegionForConsole.to("PageViewsByGenderUser", Produced.with(stringSerde, valueAvroSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
        streams.cleanUp();

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}