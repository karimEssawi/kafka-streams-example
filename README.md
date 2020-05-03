## Kafka streams top pages example:
Code walkthrough:
- All stream processing code is in the `StreamProcessor` class.
- Consumes messages off pagesviews and users topics.
- Joins messages in said topics on the user id field.
- Groups by gender and pageid and computes total view time and distinct users per gender/pageid.
- Next, it groups by gender and collects pageIds, total view time, users in a priority queue sorted by viewtime desc.
- Finally, once per minute it produces the top 10 pages by view time (one message per page) for every gender to the top_pages topic.

### To run
`./gradlew run`

### Things I would've done If I had more time:
- Use SpecificAvro instread of GenericAvro for serde - The reason I couldn't do that is becauae I couldn't get the gradle plugin for creating POJOs from avro schemas to work properly and I had spent too much time on that.
- Write unit and integration tests - As you probably know, testing streaming applications is inherently hard. Also, I'm new to kafka streams so it was a bit of a learning curve and I needed more time to figure out how to test the app.
- Write the app in scala - Although I prefer scala as it much more functional and concise, most of th tutorials and examples were in Java so I took a pragmatic approach and used the default Java DSL. 
