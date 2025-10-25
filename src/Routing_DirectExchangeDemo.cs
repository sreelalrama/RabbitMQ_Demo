using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ_Demo
{
    internal class Routing_DirectExchangeDemo
    {
        private static volatile bool keepRunning = true;

        internal void PerformDemo()
        {
            Console.WriteLine("╔═══════════════════════════════════════════════════╗");
            Console.WriteLine("║   Routing (Direct Exchange) - Selective Routing   ║");
            Console.WriteLine("╚═══════════════════════════════════════════════════╝\n");

            // Create 3 subscriber threads with different routing preferences
            Thread subscriber1 = new Thread(() =>
                SubscriberTask("Subscriber: errors only", new[] { "error" }, ConsoleColor.Red));

            Thread subscriber2 = new Thread(() =>
                SubscriberTask("Subscriber: warnings only", new[] { "warning" }, ConsoleColor.Yellow));

            Thread subscriber3 = new Thread(() =>
                SubscriberTask("Subscriber: errors + warnings", new[] { "error", "warning" }, ConsoleColor.Magenta));

            subscriber1.IsBackground = true;
            subscriber2.IsBackground = true;
            subscriber3.IsBackground = true;

            // Start all subscribers
            Console.WriteLine("🟢 Starting Subscribers with different routing keys...\n");
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
            Console.WriteLine("Press 'E' to send an ERROR message");
            Console.WriteLine("Press 'W' to send a WARNING message");
            Console.WriteLine("Press 'I' to send an INFO message");
            Console.WriteLine("Press 'A' to send ALL types (error, warning, info)");
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
                    else if (key == ConsoleKey.E)
                    {
                        Thread msg = new Thread(() => SendMessage("error", "Manual ERROR message"));
                        msg.Start();
                    }
                    else if (key == ConsoleKey.W)
                    {
                        Thread msg = new Thread(() => SendMessage("warning", "Manual WARNING message"));
                        msg.Start();
                    }
                    else if (key == ConsoleKey.I)
                    {
                        Thread msg = new Thread(() => SendMessage("info", "Manual INFO message"));
                        msg.Start();
                    }
                    else if (key == ConsoleKey.A)
                    {
                        Thread msg = new Thread(SendAllTypes);
                        msg.Start();
                    }
                }

                Thread.Sleep(100);
            }

            Thread.Sleep(2000);
            Console.WriteLine("\n✓ Application stopped. Goodbye!");
        }

        // Publisher: Sends messages with different routing keys
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
                    // Declare DIRECT exchange
                    // Direct exchange routes based on EXACT routing key match
                    channel.ExchangeDeclare(
                        exchange: "direct_logs",
                        type: ExchangeType.Direct,  // Exact routing key matching
                        durable: false,
                        autoDelete: false,
                        arguments: null
                    );

                    string[] severities = { "error", "warning", "info" };
                    int messageNumber = 1;

                    while (keepRunning)
                    {
                        // Cycle through different severity levels
                        string severity = severities[messageNumber % 3];
                        string message = $"[{severity.ToUpper()}] Log message #{messageNumber} at {DateTime.Now:HH:mm:ss}";

                        SendMessage("direct_logs", severity, message);

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

        // Helper method to send a single message with specific routing key
        static void SendMessage(string routingKey, string customMessage = null)
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
                        exchange: "direct_logs",
                        type: ExchangeType.Direct
                    );

                    string message = customMessage ?? $"[{routingKey.ToUpper()}] Message at {DateTime.Now:HH:mm:ss}";

                    SendMessage("direct_logs", routingKey, message);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ [SendMessage] ERROR: {ex.Message}");
            }
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
                        type: ExchangeType.Direct
                    );

                    var body = Encoding.UTF8.GetBytes(message);

                    // Publish with ROUTING KEY
                    // Only queues bound with this exact routing key will receive it
                    channel.BasicPublish(
                        exchange: exchange,
                        routingKey: routingKey,  // ← This determines who receives it!
                        basicProperties: null,
                        body: body
                    );

                    // Color-code output based on severity
                    ConsoleColor color = routingKey switch
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

        // Send one message of each type
        static void SendAllTypes()
        {
            Console.WriteLine("\n💥 Sending all message types...\n");
            SendMessage("error", "BURST: Critical error occurred!");
            Thread.Sleep(200);
            SendMessage("warning", "BURST: Warning - disk space low");
            Thread.Sleep(200);
            SendMessage("info", "BURST: System running normally");
            Console.WriteLine("\n✓ Burst complete!\n");
        }

        // Subscriber: Receives only messages matching its bound routing keys
        static void SubscriberTask(string subscriberName, string[] routingKeys, ConsoleColor color)
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
                    // Declare the same direct exchange
                    channel.ExchangeDeclare(
                        exchange: "direct_logs",
                        type: ExchangeType.Direct
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

                    // BIND queue to exchange for EACH routing key
                    // This is the key difference from fanout!
                    foreach (var routingKey in routingKeys)
                    {
                        channel.QueueBind(
                            queue: queueName,
                            exchange: "direct_logs",
                            routingKey: routingKey  // ← Only receive messages with this key!
                        );

                        Console.ForegroundColor = color;
                        Console.WriteLine($"   ✓ [{subscriberName}] Bound with routing_key: '{routingKey}'");
                        Console.ResetColor();
                    }

                    Console.ForegroundColor = color;
                    Console.WriteLine($"   ✅ [{subscriberName}] Ready to receive!\n");
                    Console.ResetColor();

                    var consumer = new EventingBasicConsumer(channel);

                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        var routingKey = ea.RoutingKey;  // Get the routing key

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
