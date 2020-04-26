package de.joyn;

import de.joyn.utils.PriorityQueueSerde;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxBytes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

public class StreamProcessor {
    private final StreamsBuilder builder;
    private final Properties streamsConfiguration;
    private static final Serde<Windowed<String>> windowedStringSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

    public StreamProcessor(Properties streamsConfiguration) {
        this.builder = new StreamsBuilder();
        this.streamsConfiguration = streamsConfiguration;
    }

    public KafkaStreams processStreams() throws IOException {
        final KStream<String, GenericRecord> pageViewsStream = getPageViewsStream(builder);

        final KTable<String, String> usersTable = getUsersTable(builder);

        // Specify the Avro schema for intermediate generic Avro classes.
        final InputStream pageViewGenderAvro = Main.class.getClassLoader().getResourceAsStream("avro/pageviewgender.avsc");
        final Schema pageViewGenderSchema = new Schema.Parser().parse(pageViewGenderAvro);

        final InputStream topPagesAvro = Main.class.getClassLoader().getResourceAsStream("avro/pageviewusercount.avsc");
        final Schema topPagesSchema = new Schema.Parser().parse(topPagesAvro);


        final KStream<String, GenericRecord> PageViewsAndUsers = joinStreams(pageViewsStream, usersTable, pageViewGenderSchema);
        final KTable<Windowed<String>, GenericRecord> totalViewTimeByGenderPageId = getTotalViewTime(PageViewsAndUsers, pageViewGenderSchema);
        final KTable<Windowed<String>, GenericRecord> distinctUsersByGenderPageId = getDistinctUsers(totalViewTimeByGenderPageId, topPagesSchema);
        final KStream<Windowed<String>, GenericRecord> topPages = getTopPages(distinctUsersByGenderPageId);

        sendOrderedPagesToOutputStream(topPages);

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

    protected KStream<Windowed<String>, GenericRecord> getTopPages(KTable<Windowed<String>, GenericRecord> distinctUsersByGenderPageId) {
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        final Serde<GenericRecord> valueAvroSerde = new GenericAvroSerde();
        valueAvroSerde.configure(serdeConfig, false);

        final Comparator<GenericRecord> comparator = (o1, o2) -> (int) ((Long) o2.get("totalviewtime") - (Long) o1.get("totalviewtime"));

        KTable<Windowed<String>, PriorityQueue<GenericRecord>> orderedTopPages = distinctUsersByGenderPageId
                .groupBy(
                        (k, v) -> {
                            // project on the gender field for key
                            final Windowed<String> windowedGender = new Windowed<>(v.get("gender").toString(), k.window());
                            return new KeyValue<>(windowedGender, v);
                        },
                        Grouped.with(windowedStringSerde, valueAvroSerde)
                )
                .aggregate(
                        () -> new PriorityQueue<>(comparator),

                        (windowedGender, record, queue) -> {
                            queue.add(record);
                            return queue;
                        },

                        (windowedGender, record, queue) -> {
                            queue.remove(record);
                            return queue;
                        },

                        Materialized.<String, PriorityQueue<GenericRecord>, WindowStore<Bytes, byte[]>>as("top-pages-store").with(windowedStringSerde, new PriorityQueueSerde<>(comparator, valueAvroSerde))                );

        return orderedTopPages
                .toStream()
                .flatMapValues(queue -> {
                    final List<GenericRecord> topPagesRecords = new ArrayList<>();
                    for (int i = 0; i < 10; i++) {
                        final GenericRecord record = queue.poll();
                        if (record == null) {
                            break;
                        }
                        topPagesRecords.add(record);
                    }
                    return topPagesRecords;
                });
    }

    protected KStream<String, GenericRecord> joinStreams(KStream<String, GenericRecord> pageViewsStream,
                                                         KTable<String, String> usersTable, Schema pageViewGenderSchema) {
        return pageViewsStream
                .leftJoin(usersTable, (view, gender) -> {
                    final GenericRecord viewGender = new GenericData.Record(pageViewGenderSchema);
                    viewGender.put("userid", view.get("userid"));
                    viewGender.put("pageid", view.get("pageid"));
                    viewGender.put("viewtime", view.get("viewtime"));
                    viewGender.put("gender", gender);

                    return viewGender;
                });
    }

    protected KTable<Windowed<String>, GenericRecord> getTotalViewTime(KStream<String, GenericRecord> joinedPageviewsUsers, Schema pageViewGenderSchema) {
        return joinedPageviewsUsers
                // map key to gender and pageId String - This is really hacky, Use a Pair instead?
                .map((dummy, viewGender) -> new KeyValue<>(viewGender.get("gender").toString() + viewGender.get("pageid").toString(), viewGender))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofSeconds(10)).grace(Duration.ZERO))
                // Calculate total viewtime for each gender-pageId pair and collate users to get distinct users count in downstream mapping
                .reduce((base, acc) -> {
                            final GenericRecord viewGender = new GenericData.Record(pageViewGenderSchema);
                            viewGender.put("userid", base.get("userid") + "|" + acc.get("userid"));
                            viewGender.put("pageid", base.get("pageid"));
                            viewGender.put("viewtime", Long.parseLong(base.get("viewtime").toString()) + Long.parseLong(acc.get("viewtime").toString()));
                            viewGender.put("gender", base.get("gender").toString());

                            return viewGender;
                        },
                        Materialized.<String, GenericRecord, WindowStore<Bytes, byte[]>>as("total-viewtimes-store").withKeySerde(Serdes.String())
                );
    }

    protected KTable<Windowed<String>, GenericRecord> getDistinctUsers(KTable<Windowed<String>, GenericRecord> totalViewTimeByGenderPageId, Schema topPagesSchema) {
        return totalViewTimeByGenderPageId
                .mapValues(v -> {
                    // split users by | and then collect in a set. Really hackey!!
                    HashSet<String> users = new HashSet<>(Arrays.asList(v.get("userid").toString().split("\\|")));
                    final GenericRecord viewGender = new GenericData.Record(topPagesSchema);
                    viewGender.put("gender", v.get("gender"));
                    viewGender.put("pageid", v.get("pageid").toString());
                    viewGender.put("totalviewtime", v.get("viewtime"));
                    viewGender.put("usercount", users.size());

                    return viewGender;
                });
    }

    private void sendToOutputStream(KTable<Windowed<String>, GenericRecord> distinctUsersByGenderPageId) {
        distinctUsersByGenderPageId
                .suppress(untilWindowCloses(unbounded()))
                .toStream((window, value) -> window.toString())
                .to("top_pages");
    }

    private void sendOrderedPagesToOutputStream(KStream<Windowed<String>, GenericRecord> distinctUsersByGenderPageId) {
        distinctUsersByGenderPageId
                .toTable()
//                .suppress(untilWindowCloses(unbounded()))
                // suppress untilWindowCloses doesn't emit any records for some reason so I used untilTimeLimit instead, but in a production env I would dig into the issue till I find the problem
                .suppress(untilTimeLimit(Duration.ofMinutes(1), maxBytes(100000000L).emitEarlyWhenFull()))
                .toStream((window, value) -> window.toString())
                .to("top_pages");
    }

    protected KTable<String, String> getUsersTable(StreamsBuilder builder) {
        // Create a changelog stream as a projection of the value to the gender attribute only
        final KTable<String, GenericRecord> userProfiles = builder.table("users");
        return userProfiles.mapValues(record -> record.get("gender").toString());
    }

    protected KStream<String, GenericRecord> getPageViewsStream(StreamsBuilder builder) {
        // Create a keyed stream of page view events from the PageViews stream,
        // by extracting the user id from the Avro value
        KStream<String, GenericRecord> pageViews = builder.stream("pageviews");
        return pageViews.map((dummy, record) -> new KeyValue<>(record.get("userid").toString(), record));
    }
}
