using Confluent.Kafka;

class kafka_transactions_example
{
    private static async Task Main(string[] args)
    {
        var producerConfig = new ProducerConfig { BootstrapServers = "127.0.0.1:49695", TransactionalId = "johnathan"};
        using var p = new ProducerBuilder<Null, string>(producerConfig).Build();
        p.InitTransactions(TimeSpan.FromSeconds(1));
        
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "127.0.0.1:49695",
            GroupId = "johnathan-consumer-group",
            IsolationLevel = IsolationLevel.ReadCommitted
        };
        using var c = new ConsumerBuilder<Null, string>(consumerConfig).Build();
        
        // Produce to consumer topic
        p.BeginTransaction();
        for (var i = 0; i < 1000; i++)
        {
            await p.ProduceAsync("test-consume-topic", new Message<Null, string>{Value = i.ToString()});
        }
        p.CommitTransaction();
        
        // Consume from consumer-topic and produce to another topic showcasing the transactional messaging.
        while (true)
        {
            p.BeginTransaction();
            // Do some consuming here.
            var consumeResult = c.Consume();
            p.SendOffsetsToTransaction(new List<TopicPartitionOffset>
            {
                consumeResult.TopicPartitionOffset
            }, null, TimeSpan.FromSeconds(2));
            
            
            try
            {
                var dr = await p.ProduceAsync("test-topic", new Message<Null, String> { Value = "consumed " + consumeResult.Message.Value });
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
            p.CommitTransaction();
            await Task.Delay(1000);
        }
    }
}