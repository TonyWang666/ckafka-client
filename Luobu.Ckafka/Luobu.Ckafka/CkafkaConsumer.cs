using System;
using Confluent.Kafka;
using System.Threading;

namespace Luobu.Ckafka
{
    public class CkafkaConsumer
    {
        private string _CkafkaAddress;
        private string _GroupId;
        private IConsumer<Ignore, string> _Consumer;

        public CkafkaConsumer()
        {
        }

        public CkafkaConsumer(string groupId, string cKafkaAddress)
        {
            _GroupId = groupId ?? throw new ArgumentException(nameof(groupId));
            _CkafkaAddress = cKafkaAddress ?? throw new ArgumentNullException(nameof(cKafkaAddress));
            BuildConsumer();
        }

        private void BuildConsumer()
        {
            var config = new ConsumerConfig {
                GroupId = _GroupId,
                BootstrapServers = _CkafkaAddress,
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so consumption will only start from the
                // earliest message in the topic the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            _Consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        }

        public ConsumeResult<Ignore, string> GetCkafkaMessagesAsync(string topicName){
            _Consumer.Subscribe(topicName);
            CancellationTokenSource cts = new CancellationTokenSource();
            var result = _Consumer.Consume(cts.Token);
            _Consumer.Unsubscribe();
            return result;
        }
    }
}
