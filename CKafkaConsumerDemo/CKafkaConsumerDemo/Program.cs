using System;
using Confluent.Kafka;
using Luobu.CkafkaClient;


namespace CKafkaConsumerDemo
{
    /*
     * Description: A program to run as consumer of Tencent Cloud CKafka
     * This program calls CkafkaConsumer from Luobu.Ckafka
     */
    public class Program
    {
        public static void Main(string[] args)
        {
            while (true)
            {
                CkafkaConsumer consumer = new CkafkaConsumer("tns-event-processor-consumer", "172.20.244.15:9092");
                ConsumeResult<Ignore, string> res = consumer.GetCkafkaMessagesAsync("topic-tns-dispatcher");
                Console.WriteLine($"Consumed message '{res.Message.Value}' at: '{res.TopicPartitionOffset}'.");
            }
        }
    }
}
