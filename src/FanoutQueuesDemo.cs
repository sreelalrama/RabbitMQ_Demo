using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ_Demo
{
    internal class FanoutQueuesDemo
    {
        private static volatile bool keepRunning = true;
        internal void PerformDemo()
        {
            Console.WriteLine("╔═══════════════════════════════════════════════════╗");
            Console.WriteLine("║  Publish/Subscribe (Fanout) - Broadcast Pattern   ║");
            Console.WriteLine("╚═══════════════════════════════════════════════════╝\n");

            // Create 3 subscriber threads
            Thread subscriber1 = new Thread(() => SubscriberTask("Subscriber 1", ConsoleColor.Green));
            Thread subscriber2 = new Thread(() => SubscriberTask("Subscriber 2", ConsoleColor.Cyan));
            Thread subscriber3 = new Thread(() => SubscriberTask("Subscriber 3", ConsoleColor.Yellow));

            subscriber1.IsBackground = true;
            subscriber2.IsBackground = true;
            subscriber3.IsBackground = true;

            // Start all subscribers first
            Console.WriteLine("🟢 Starting Subscribers...\n");
            subscriber1.Start();
            Thread.Sleep(500);
            subscriber2.Start();
            Thread.Sleep(500);
            subscriber3.Start();

            // Wait for subscribers to initialize
            Thread.Sleep(2000);

            // Create publisher thread
            Thread publisher = new Thread(PublisherTask);
            publisher.IsBackground = true;

            Console.WriteLine("\n🔵 Starting Publisher...\n");
            publisher.Start();

            // Interactive controls
            Console.WriteLine("═══════════════════════════════════════════════════");
            Console.WriteLine("Press 'Q' to quit");
            Console.WriteLine("Press 'M' to send a single message");
            Console.WriteLine("Press 'B' to send a burst of 5 messages");
            Console.WriteLine("═══════════════════════════════════════════════════\n");

            while (keepRunning)
            {
                if (Console.KeyAvailable)
                {
                    var key = Console.ReadKey(true).Key;

                    if (key == ConsoleKey.Q)
                    {
                        Console.WriteLine("\n🛑 Shutting down...");
                        keepRunning = false;
                    }
                    else if (key == ConsoleKey.M)
                    {
                        Thread singleMsg = new Thread(() => SendSingleMessage());
                        singleMsg.Start();
                    }
                    else if (key == ConsoleKey.B)
                    {
                        Thread burst = new Thread(() => SendMessageBurst(5));
                        burst.Start();
                    }
                }

                Thread.Sleep(100);
            }

            Thread.Sleep(2000);
            Console.WriteLine("\n✓ Application stopped. Goodbye!");
        }

        // Publisher: Broadcasts messages to all subscribers via Fanout exchange
        static void PublisherTask()
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };

            try
            {
                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    // Declare FANOUT exchange
                    // Fanout broadcasts to ALL bound queues (ignores routing key)
                    channel.ExchangeDeclare(
                        exchange: "logs",
                        type: ExchangeType.Fanout,  // Broadcast to all
                        durable: false,
                        autoDelete: false,
                        arguments: null
                    );

                    int messageNumber = 1;

                    while (keepRunning)
                    {
                        string message = $"Message {messageNumber}: Log at {DateTime.Now:HH:mm:ss}";
                        var body = Encoding.UTF8.GetBytes(message);

                        // Publish to exchange (not directly to queue)
                        // Routing key is ignored for fanout exchanges
                        channel.BasicPublish(
                            exchange: "logs",       // Send to exchange
                            routingKey: "",         // Ignored for fanout
                            basicProperties: null,
                            body: body
                        );

                        Console.ForegroundColor = ConsoleColor.Blue;
                        Console.WriteLine($" [Publisher] Broadcast: '{message}'");
                        Console.ResetColor();

                        messageNumber++;

                        // Send a message every 4 seconds
                        Thread.Sleep(4000);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ [Publisher] ERROR: {ex.Message}");
            }
        }

        // Send a single message on demand
        static void SendSingleMessage()
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };

            try
            {
                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(
                        exchange: "logs",
                        type: ExchangeType.Fanout
                    );

                    string message = $"MANUAL Message at {DateTime.Now:HH:mm:ss.fff}";
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(
                        exchange: "logs",
                        routingKey: "",
                        basicProperties: null,
                        body: body
                    );

                    Console.ForegroundColor = ConsoleColor.Magenta;
                    Console.WriteLine($"\n💬 [Manual] Broadcast: '{message}'\n");
                    Console.ResetColor();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ [Manual] ERROR: {ex.Message}");
            }
        }

        // Send a burst of messages
        static void SendMessageBurst(int count)
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };

            try
            {
                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(
                        exchange: "logs",
                        type: ExchangeType.Fanout
                    );

                    Console.ForegroundColor = ConsoleColor.Magenta;
                    Console.WriteLine($"\n💥 [Burst] Sending {count} messages...");
                    Console.ResetColor();

                    for (int i = 1; i <= count; i++)
                    {
                        string message = $"BURST Message {i} at {DateTime.Now:HH:mm:ss.fff}";
                        var body = Encoding.UTF8.GetBytes(message);

                        channel.BasicPublish(
                            exchange: "logs",
                            routingKey: "",
                            basicProperties: null,
                            body: body
                        );

                        Console.ForegroundColor = ConsoleColor.Magenta;
                        Console.WriteLine($"   💥 Sent: '{message}'");
                        Console.ResetColor();

                        Thread.Sleep(200);
                    }

                    Console.ForegroundColor = ConsoleColor.Magenta;
                    Console.WriteLine($"✓ Burst complete!\n");
                    Console.ResetColor();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ [Burst] ERROR: {ex.Message}");
            }
        }

        // Subscriber: Receives ALL messages from the exchange
        static void SubscriberTask(string subscriberName, ConsoleColor color)
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };

            try
            {
                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    // Declare the same fanout exchange
                    channel.ExchangeDeclare(
                        exchange: "logs",
                        type: ExchangeType.Fanout
                    );

                    // Create a TEMPORARY, EXCLUSIVE queue
                    // RabbitMQ auto-generates a unique name (e.g., "amq.gen-JzTY20BRgKO...")
                    // exclusive: true - only THIS connection can access it
                    // autoDelete: true - deleted when connection closes
                    var queueDeclareResult = channel.QueueDeclare(
                        queue: "",              // Auto-generate name
                        durable: false,         // Don't persist
                        exclusive: true,        // Only for this connection
                        autoDelete: true,       // Delete when disconnected
                        arguments: null
                    );

                    // Get the auto-generated queue name
                    string queueName = queueDeclareResult.QueueName;

                    Console.ForegroundColor = color;
                    Console.WriteLine($"   📬 [{subscriberName}] Created queue: {queueName}");
                    Console.ResetColor();

                    // BIND queue to exchange
                    // This is the key step that connects the subscriber to the broadcast
                    channel.QueueBind(
                        queue: queueName,       // My unique queue
                        exchange: "logs",       // The fanout exchange
                        routingKey: ""          // Ignored for fanout
                    );

                    Console.ForegroundColor = color;
                    Console.WriteLine($"   ✓ [{subscriberName}] Bound to 'logs' exchange - Ready!");
                    Console.ResetColor();

                    var consumer = new EventingBasicConsumer(channel);

                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);

                        Console.ForegroundColor = color;
                        Console.WriteLine($"   📨 [{subscriberName}] Received: '{message}'");
                        Console.ResetColor();
                    };

                    // Start consuming
                    channel.BasicConsume(
                        queue: queueName,
                        autoAck: true,          // Auto-acknowledge (simpler for pub/sub)
                        consumer: consumer
                    );

                    // Keep subscriber alive
                    while (keepRunning)
                    {
                        Thread.Sleep(1000);
                    }

                    Console.ForegroundColor = color;
                    Console.WriteLine($"   🛑 [{subscriberName}] Shutting down...");
                    Console.ResetColor();
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = color;
                Console.WriteLine($"❌ [{subscriberName}] ERROR: {ex.Message}");
                Console.ResetColor();
            }
        }
    }
}
