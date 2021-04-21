using System;
using Confluent.Kafka;
using Luobu.Ckafka;

namespace CkafkaConsumerTest
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
            Console.WriteLine("Program End...");

            //Below code are consumer test successfully before:
            //var conf = new ConsumerConfig
            //{
            //    // Number of ConsumerInstance in ConsumerGourp must be less than num of Partitions
            //    GroupId = "tns-event-processor-consumer",
            //    BootstrapServers = "172.20.244.15:9092",
            //    // Note: The AutoOffsetReset property determines the start offset in the event
            //    // there are not yet any committed offsets for the consumer group for the
            //    // topic/partitions of interest. By default, offsets are committed
            //    // automatically, so in this example, consumption will only start from the
            //    // earliest message in the topic 'my-topic' the first time you run the program.
            //    AutoOffsetReset = AutoOffsetReset.Earliest
            //};


            //using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            //{
            //    c.Subscribe("topic-tns-dispatcher");

            //    CancellationTokenSource cts = new CancellationTokenSource();
            //    Console.CancelKeyPress += (_, e) => {
            //        e.Cancel = true; // prevent the process from terminating.
            //        cts.Cancel();
            //    };

            //    try
            //    {
            //        while (true)
            //        {
            //            try
            //            {
            //                var cr = c.Consume(cts.Token);
            //                Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
            //            }
            //            catch (ConsumeException e)
            //            {
            //                Console.WriteLine($"Error occured: {e.Error.Reason}");
            //            }
            //        }
            //    }
            //    catch (OperationCanceledException)
            //    {
            //        // Ensure the consumer leaves the group cleanly and final offsets are committed.
            //        c.Close();
            //    }
            //}
        }
    }
}
