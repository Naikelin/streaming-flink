from pyflink.common import Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer


def dsWaze():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///home/nk/U/streaming-flink/flink-sql-connector-kafka-1.15.2.jar")
    env.set_parallelism(1)

    kafka_source = FlinkKafkaConsumer(
        topics='events',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'CG1'})

    ds = env.add_source(kafka_source)

    ds.map(lambda x: Row(x), output_type=Types.ROW([Types.STRING()])) \
        .print()

    env.execute('streaming events')


if __name__ == '__main__':
    dsWaze()