using System;
using System.Threading;
using Confluent.Kafka;
using Luobu.CKafka;

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
            CKafkaConsumer consumer = new CKafkaConsumer("172.20.244.15:9092", "tns-event-processor-consumer", "topic-tns-dispatcher");
            try
            {
                int num = 1;
                while (true)
                {
                    CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
                    ConsumeResult<Ignore, string> consumeResult = consumer.GetCkafkaMessagesAsync(cancellationTokenSource.Token);
                    if (consumeResult == null || consumeResult.IsPartitionEOF)
                    {
                        Console.WriteLine("Received NULL");
                        continue;
                    }
                    Console.WriteLine($"Consumed message '{consumeResult.Message.Value}' at: " +
                        $"'{consumeResult.TopicPartitionOffset}'.");
                    Console.WriteLine($"Consumed message {num++} time");
                    consumer.Commit();
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Exception in main is: {ex}");
            }
            finally
            {
                Console.WriteLine("Closing main...");
                consumer.Close();
            }
        }
    }
}
