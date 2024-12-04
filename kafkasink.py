from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import KafkaSource
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema, DeliveryGuarantee
#import redis

def read_config():
  # reads the client configuration from poc.properties
  # and returns it as a key-value map
  config = {}
  with open("poc.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

#print(read_config())

def kafka_source(*args):
  print (args[2])

  # Create a StreamExecutionEnvironment
  env = StreamExecutionEnvironment.get_execution_environment()
  print ('env executed')
  print(args[2]["bootstrap.servers"])

  # Adding the jar to my streaming environment.
  env.add_jars("file:////opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar")

  earliest = False
  offset = KafkaOffsetsInitializer.earliest() if earliest else KafkaOffsetsInitializer.latest()

  # Create a Kafka Source
  kafka_source = KafkaSource.builder() \
  .set_topics(args[0]) \
  .set_properties(args[2]) \
  .set_starting_offsets(offset) \
  .set_value_only_deserializer(SimpleStringSchema())\
  .build()

  # Create a DataStream from the Kafka source and assign timestamps and watermarks
  #data_stream = env.from_source(kafka_source, WatermarkStrategy.for_monotonous_timestamps(), "Kafka Source")
  data_stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

  # Print line for readablity in the console
  print("start reading data from kafka")

  data_stream.print()
  

  sink = KafkaSink.builder() \
    .set_bootstrap_servers(args[2]["bootstrap.servers"]) \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic(args[1])
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
    ) \
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    .build()

  data_stream.sink_to(sink)


  # Execute the Flink pipeline
  env.execute("Kafka Source Execution")

def main():
  config = read_config()
  source_topic = "topic_a"
  sink_topic = "topic_b"

  kafka_source(source_topic, sink_topic, config)

if __name__ == '__main__':
  main()
