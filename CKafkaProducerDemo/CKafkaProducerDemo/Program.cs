using System;
using Confluent.Kafka;
using System.Threading.Tasks;
using Luobu.Ckafka;

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
            // Send infinited time message to topic "topic-tns-dispatcher" with CKafka address "172.20.244.15:9092"
            //while (true)
            //{
            //    CKafkaProducer producer = new CKafkaProducer("172.20.244.15:9092", "topic-tns-dispatcher");
            //    DeliveryResult<Null, string> result = await producer.PublishMessageAsync("testData741-1");
            //    Console.WriteLine($"Delivered '{result.Value}' to '{result.TopicPartitionOffset}'");
            //}

            // Send 100 times incremented message
            //for(int i = 0; i < 100; i++)
            //{
            //    CKafkaProducer producer = new CKafkaProducer("172.20.244.15:9092", "topic-tns-dispatcher");
            //    string message = "testData1113-" + i;
            //    DeliveryResult<Null, string> result = await producer.PublishMessageAsync(message);
            //    Console.WriteLine($"Delivered '{result.Value}' to '{result.TopicPartitionOffset}'");
            //}

            // Send 1 Message to topic "topic-tns-dispatcher"
            CKafkaProducer producer = new CKafkaProducer("172.20.244.15:9092", "topic-tns-dispatcher");
            DeliveryResult<Null, string> result = await producer.PublishMessageAsync("testData1204-1");
            Console.WriteLine($"Delivered '{result.Value}' to '{result.TopicPartitionOffset}'");

        }
    }
}
