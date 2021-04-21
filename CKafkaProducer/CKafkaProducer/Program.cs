using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Luobu.Ckafka;

namespace CkafkaProducerTest
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
            DeliveryResult<Null, string> result = await producer.PublishMessageAsync("testData3");
            Console.WriteLine($"Delivered '{result.Value}' to '{result.TopicPartitionOffset}'");

            // Below code are producer test successfully before:
            //var config = new ProducerConfig { BootstrapServers = "172.20.244.15:9092" };

            //// If serializers are not specified, default serializers from
            //// `Confluent.Kafka.Serializers` will be automatically used where
            //// available. Note: by default strings are encoded as UTF8.
            //using (var p = new ProducerBuilder<Null, string>(config).Build())
            //{
            //    try
            //    {
            //        // Put topicName here!!!
            //        //Don't put topicId here!!! Or CKafka will create a new topic with topicName you input.
            //        var dr = await p.ProduceAsync("topic-tns-dispatcher", new Message<Null, string> { Value = "testData2" });
            //        Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            //    }
            //    catch (ProduceException<Null, string> e)
            //    {
            //        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            //    }
            //}
        }
    }
}
