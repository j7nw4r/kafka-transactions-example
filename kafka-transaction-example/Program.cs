using Confluent.Kafka;

internal class kafka_transactions_example
{
    private const string CONSUMER_TOPIC_NAME = "test-consume-topic";

    private static async Task Main(string[] args)
    {
        var producerConfig = new ProducerConfig { BootstrapServers = "127.0.0.1:49695", TransactionalId = "johnathan" };
        using var p = new ProducerBuilder<Null, string>(producerConfig).Build();
        p.InitTransactions(TimeSpan.FromSeconds(1));

        var commitedConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = "127.0.0.1:49695",
            GroupId = "johnathan-consumer-group",
            IsolationLevel = IsolationLevel.ReadCommitted,
            AllowAutoCreateTopics = true,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        using var committedConsumer = new ConsumerBuilder<Null, string>(commitedConsumerConfig).Build();
        committedConsumer.Subscribe(CONSUMER_TOPIC_NAME);
        
        var uncommittedConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = "127.0.0.1:49695",
            GroupId = "johnathan-consumer-group",
            IsolationLevel = IsolationLevel.ReadCommitted,
            AllowAutoCreateTopics = true,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        using var uncommittedConsumer = new ConsumerBuilder<Null, string>(uncommittedConsumerConfig).Build();
        committedConsumer.Subscribe(CONSUMER_TOPIC_NAME);

        
        // Produce to consumer topic
        await PrimeConsumerTopic(p, CONSUMER_TOPIC_NAME);

        // Consume from consumer-topic and produce to another topic showcasing the transactional messaging.
        while (true)
        {
            p.BeginTransaction();
            
            // Do some consuming here.
            var consumeResult = committedConsumer.Consume();
            Console.WriteLine("consumed " + consumeResult.Message.Value);

            try
            {
                var dr = await p.ProduceAsync("test-topic",
                    new Message<Null, string> { Value = "consumed " + consumeResult.Message.Value });
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
            
            p.SendOffsetsToTransaction(
                new List<TopicPartitionOffset>
                {
                    consumeResult.TopicPartitionOffset
                }, committedConsumer.ConsumerGroupMetadata,
                TimeSpan.FromSeconds(2));

            p.CommitTransaction();
            
            consumeResult = committedConsumer.Consume();
            committedConsumer.Commit();
            
            await Task.Delay(1000);
        }
    }

    private static async Task ConsumeAsync(IConsumer<Null, string> c, string topic)
    {
        c.Subscribe(topic);

        while (true)
        {
            var cr = c.Consume();
            if (cr.IsPartitionEOF)
            {
                await Task.Yield();
                continue;
            }
            
            Console.WriteLine("consumed ");
            
        }
    }

    private static async Task ProduceAsync()
    {
        
    }


    private static async Task PrimeConsumerTopic(IProducer<Null, string> p, string consumerTopicName)
    {
        Random rand = new();
        for (var x = 0; x < 100; x++)
        {
            p.BeginTransaction();
            for (var i = 0; i < rand.Next(1, 10); i++)
            {
                var dr = await p.ProduceAsync(CONSUMER_TOPIC_NAME, new Message<Null, string> { Value = "primed " + i });
                if (dr.Status == PersistenceStatus.Persisted)
                {
                    continue;
                }

                p.AbortTransaction();
                break;
            }

            p.CommitTransaction();
        }
        
        Console.WriteLine("done priming");
    }
}