using System;
using Confluent.Kafka;
using Luobu.CkafkaClient;
using System.Threading.Tasks;

namespace CKafkaProducerDemo
{
    /*
     * Description: A program to run as producer of Tencent Cloud CKafka
     * This program calls CkafkaProducer from Luobu.Ckafka.
     * More Detail at: https://github.com/confluentinc/confluent-kafka-dotnet
     */
    public class Program
    {
        public static async Task Main(string[] args)
        {
            CkafkaProducer producer = new CkafkaProducer("172.20.244.15:9092", "topic-tns-dispatcher");
            DeliveryResult<Null, string> result = await producer.PublishMessageAsync("testData4");
            Console.WriteLine($"Delivered '{result.Value}' to '{result.TopicPartitionOffset}'");
        }
    }
}
