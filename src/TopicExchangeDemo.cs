using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ_Demo
{
    internal class TopicExchangeDemo
    {
        private static volatile bool keepRunning = true;

        internal void PerformDemo()
        {
            Console.WriteLine("╔═══════════════════════════════════════════════════╗");
            Console.WriteLine("║   Topic Exchange - Wildcard Pattern Matching     ║");
            Console.WriteLine("╚═══════════════════════════════════════════════════╝\n");

            Console.WriteLine("📖 Pattern Guide:");
            Console.WriteLine("   * (star)  = matches exactly ONE word");
            Console.WriteLine("   # (hash)  = matches ZERO or more words");
            Console.WriteLine("   Example: 'auth.error' has 2 words: 'auth' and 'error'\n");

            // Create 3 subscriber threads with different pattern subscriptions
            Thread subscriber1 = new Thread(() =>
                SubscriberTask("Subscriber: all errors", new[] { "*.error" }, ConsoleColor.Red));

            Thread subscriber2 = new Thread(() =>
                SubscriberTask("Subscriber: all auth messages", new[] { "auth.#" }, ConsoleColor.Cyan));

            Thread subscriber3 = new Thread(() =>
                SubscriberTask("Subscriber: all messages", new[] { "#" }, ConsoleColor.Green));

            subscriber1.IsBackground = true;
            subscriber2.IsBackground = true;
            subscriber3.IsBackground = true;

            // Start all subscribers
            Console.WriteLine("🟢 Starting Subscribers with pattern bindings...\n");
            subscriber1.Start();
            Thread.Sleep(500);
            subscriber2.Start();
            Thread.Sleep(500);
            subscriber3.Start();

            Thread.Sleep(2000);

            // Create publisher thread
            Thread publisher = new Thread(PublisherTask);
            publisher.IsBackground = true;

            Console.WriteLine("\n🔵 Starting Publisher...\n");
            publisher.Start();

            // Interactive controls
            Console.WriteLine("═══════════════════════════════════════════════════");
            Console.WriteLine("Press 'Q' to quit");
            Console.WriteLine("Press '1' to send: auth.error");
            Console.WriteLine("Press '2' to send: database.warning");
            Console.WriteLine("Press '3' to send: api.info");
            Console.WriteLine("Press '4' to send: auth.info.detailed");
            Console.WriteLine("Press 'A' to send ALL example messages");
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
                    else if (key == ConsoleKey.D1)
                    {
                        Thread msg = new Thread(() => SendMessage("auth.error", "Authentication failed!"));
                        msg.Start();
                    }
                    else if (key == ConsoleKey.D2)
                    {
                        Thread msg = new Thread(() => SendMessage("database.warning", "Connection pool exhausted"));
                        msg.Start();
                    }
                    else if (key == ConsoleKey.D3)
                    {
                        Thread msg = new Thread(() => SendMessage("api.info", "Request processed successfully"));
                        msg.Start();
                    }
                    else if (key == ConsoleKey.D4)
                    {
                        Thread msg = new Thread(() => SendMessage("auth.info.detailed", "User login from new device"));
                        msg.Start();
                    }
                    else if (key == ConsoleKey.A)
                    {
                        Thread msg = new Thread(SendAllExamples);
                        msg.Start();
                    }
                }

                Thread.Sleep(100);
            }

            Thread.Sleep(2000);
            Console.WriteLine("\n✓ Application stopped. Goodbye!");
        }

        // Publisher: Sends messages with different routing keys (dot-separated words)
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
                    // Declare TOPIC exchange
                    // Topic exchange uses pattern matching with wildcards
                    channel.ExchangeDeclare(
                        exchange: "topic_logs",
                        type: ExchangeType.Topic,  // Pattern-based routing with * and #
                        durable: false,
                        autoDelete: false,
                        arguments: null
                    );

                    // Predefined routing keys matching the diagram
                    string[] routingKeys =
                    {
                        "auth.error",           // Matches: *.error, auth.#, #
                        "database.warning",     // Matches: #
                        "api.info",             // Matches: #
                        "auth.info",            // Matches: auth.#, #
                        "database.error",       // Matches: *.error, #
                        "api.error"             // Matches: *.error, #
                    };

                    int messageNumber = 1;

                    while (keepRunning)
                    {
                        // Cycle through routing keys
                        string routingKey = routingKeys[(messageNumber - 1) % routingKeys.Length];
                        string message = $"[{routingKey}] Message #{messageNumber} at {DateTime.Now:HH:mm:ss}";

                        SendMessage("topic_logs", routingKey, message);

                        messageNumber++;
                        Thread.Sleep(3000);  // Send every 3 seconds
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ [Publisher] ERROR: {ex.Message}");
            }
        }

        // Helper to send a specific message
        static void SendMessage(string routingKey, string customMessage = null)
        {
            string message = customMessage ?? $"[{routingKey}] Message at {DateTime.Now:HH:mm:ss}";
            SendMessage("topic_logs", routingKey, message);
        }

        // Core send method
        static void SendMessage(string exchange, string routingKey, string message)
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
                        exchange: exchange,
                        type: ExchangeType.Topic
                    );

                    var body = Encoding.UTF8.GetBytes(message);

                    // Publish with routing key (dot-separated words)
                    channel.BasicPublish(
                        exchange: exchange,
                        routingKey: routingKey,  // ← Pattern will be matched against bindings
                        basicProperties: null,
                        body: body
                    );

                    // Color-code based on last word (severity)
                    string[] parts = routingKey.Split('.');
                    string lastWord = parts[parts.Length - 1];

                    ConsoleColor color = lastWord switch
                    {
                        "error" => ConsoleColor.Red,
                        "warning" => ConsoleColor.Yellow,
                        "info" => ConsoleColor.Cyan,
                        _ => ConsoleColor.White
                    };

                    Console.ForegroundColor = color;
                    Console.WriteLine($"📤 [Publisher] routing_key: '{routingKey}' → '{message}'");
                    Console.ResetColor();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ [SendMessage] ERROR: {ex.Message}");
            }
        }

        // Send all example messages
        static void SendAllExamples()
        {
            Console.WriteLine("\n💥 Sending all example messages...\n");

            SendMessage("auth.error", "Authentication failed!");
            Thread.Sleep(300);

            SendMessage("database.warning", "Connection pool exhausted");
            Thread.Sleep(300);

            SendMessage("api.info", "Request processed successfully");
            Thread.Sleep(300);

            SendMessage("auth.info.detailed", "User login from new device");
            Thread.Sleep(300);

            SendMessage("database.error", "Query timeout exceeded");
            Thread.Sleep(300);

            SendMessage("api.error", "Rate limit exceeded");

            Console.WriteLine("\n✓ All examples sent!\n");
        }

        // Subscriber: Receives messages matching pattern(s)
        static void SubscriberTask(string subscriberName, string[] bindingPatterns, ConsoleColor color)
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
                    // Declare the same topic exchange
                    channel.ExchangeDeclare(
                        exchange: "topic_logs",
                        type: ExchangeType.Topic
                    );

                    // Create temporary, exclusive queue
                    var queueDeclareResult = channel.QueueDeclare(
                        queue: "",
                        durable: false,
                        exclusive: true,
                        autoDelete: true,
                        arguments: null
                    );

                    string queueName = queueDeclareResult.QueueName;

                    Console.ForegroundColor = color;
                    Console.WriteLine($"   📬 [{subscriberName}] Created queue: {queueName}");
                    Console.ResetColor();

                    // BIND queue to exchange with PATTERN(s)
                    // This is where the magic happens!
                    foreach (var pattern in bindingPatterns)
                    {
                        channel.QueueBind(
                            queue: queueName,
                            exchange: "topic_logs",
                            routingKey: pattern  // ← Pattern with wildcards (* or #)
                        );

                        Console.ForegroundColor = color;
                        Console.WriteLine($"   ✓ [{subscriberName}] Bound with pattern: '{pattern}'");
                        Console.ResetColor();
                    }

                    Console.ForegroundColor = color;
                    Console.WriteLine($"   ✅ [{subscriberName}] Ready to receive matching messages!\n");
                    Console.ResetColor();

                    var consumer = new EventingBasicConsumer(channel);

                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        var routingKey = ea.RoutingKey;

                        Console.ForegroundColor = color;
                        Console.WriteLine($"   📨 [{subscriberName}] Received [{routingKey}]: '{message}'");
                        Console.ResetColor();
                    };

                    channel.BasicConsume(
                        queue: queueName,
                        autoAck: true,
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
