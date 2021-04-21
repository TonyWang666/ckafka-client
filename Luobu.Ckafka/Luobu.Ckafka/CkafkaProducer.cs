using System;
using Confluent.Kafka;
using System.Threading.Tasks;

namespace Luobu.Ckafka
{
    public class CkafkaProducer
    {
        private string _CkafkaAddress;
        private string _TopicName;
        private IProducer<Null, string> _Producer;

        public CkafkaProducer()
        {
        }

        public CkafkaProducer(string cKafkaAddress, string topicName)
        {
            _CkafkaAddress = cKafkaAddress ?? throw new ArgumentNullException(nameof(cKafkaAddress));
            _TopicName = topicName ?? throw new ArgumentNullException(nameof(topicName));
            BuildProducer();
        }

        private void BuildProducer()
        {
            var config = new ProducerConfig { BootstrapServers = _CkafkaAddress };
            _Producer = new ProducerBuilder<Null, string>(config).Build();
        }

        public async Task<DeliveryResult<Null, string>> PublishMessageAsync(string message)
        {
            //ProduceException<Null, string> might be generated
            return await _Producer.ProduceAsync(_TopicName, new Message<Null, string> { Value = message });
        }
    }
}
