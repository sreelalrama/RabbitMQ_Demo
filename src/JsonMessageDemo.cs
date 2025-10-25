using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace RabbitMQ_Demo
{
    public class OrderMessage
    {
        public string OrderId { get; set; }
        public string Customer { get; set; }
        public List<string> Items { get; set; }
        public decimal Total { get; set; }
        public DateTime OrderDate { get; set; }
        public string Status { get; set; }

        // Constructor for easy object creation
        public OrderMessage()
        {
            Items = new List<string>();
            OrderDate = DateTime.UtcNow;
            Status = "Pending";
        }

        // Display method for pretty printing
        public override string ToString()
        {
            return $"Order #{OrderId} - Customer: {Customer}, Items: {Items.Count}, Total: ${Total:F2}, Status: {Status}";
        }
    }


    internal class JsonMessageDemo
    {
        private static volatile bool keepRunning = true;

        internal void PerformDemo()
        {
            Console.WriteLine("╔═══════════════════════════════════════════════════╗");
            Console.WriteLine("║       JSON Messages - Producer/Consumer Demo      ║");
            Console.WriteLine("╚═══════════════════════════════════════════════════╝\n");

            // Create consumer thread (starts first to be ready)
            Thread consumerThread = new Thread(ConsumerWork);
            consumerThread.Name = "Consumer Thread";
            consumerThread.IsBackground = true;

            Console.WriteLine("🟢 Starting Consumer...\n");
            consumerThread.Start();

            Thread.Sleep(2000);

            // Create producer thread
            Thread producerThread = new Thread(ProducerWork);
            producerThread.Name = "Producer Thread";
            producerThread.IsBackground = true;

            Console.WriteLine("🔵 Starting Producer...\n");
            producerThread.Start();

            // Interactive controls
            Console.WriteLine("═══════════════════════════════════════════════════");
            Console.WriteLine("Press 'Q' to quit");
            Console.WriteLine("Press 'S' to send a single order");
            Console.WriteLine("Press 'B' to send a burst of 5 orders");
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
                    else if (key == ConsoleKey.S)
                    {
                        Thread singleOrder = new Thread(() => SendSingleOrder());
                        singleOrder.Start();
                    }
                    else if (key == ConsoleKey.B)
                    {
                        Thread burstOrders = new Thread(() => SendOrderBurst(5));
                        burstOrders.Start();
                    }
                }

                Thread.Sleep(100);
            }

            Thread.Sleep(2000);
            Console.WriteLine("\n✓ Application stopped. Goodbye!");
        }

        // Producer: Creates JSON objects and sends them to the queue
        static void ProducerWork()
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
                    // Declare orders queue
                    channel.QueueDeclare(
                        queue: "orders",
                        durable: true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null
                    );

                    int orderNumber = 1;
                    string[] customers = { "John Doe", "Jane Smith", "Bob Johnson", "Alice Williams", "Charlie Brown" };
                    string[][] itemsList =
                    {
                        new[] { "Laptop", "Mouse", "Keyboard" },
                        new[] { "Phone", "Charger" },
                        new[] { "Monitor", "HDMI Cable" },
                        new[] { "Headphones", "Microphone" },
                        new[] { "Webcam" }
                    };

                    while (keepRunning)
                    {
                        // Create OrderMessage object
                        var order = new OrderMessage
                        {
                            OrderId = $"ORD-{orderNumber:D5}",
                            Customer = customers[(orderNumber - 1) % customers.Length],
                            Items = new List<string>(itemsList[(orderNumber - 1) % itemsList.Length]),
                            Total = 99.99m * orderNumber % 500 + 50m,
                            OrderDate = DateTime.UtcNow,
                            Status = "Pending"
                        };

                        // Serialize to JSON
                        string jsonString = JsonSerializer.Serialize(order, new JsonSerializerOptions
                        {
                            WriteIndented = true  // Pretty print for readability
                        });

                        // Convert JSON string to bytes
                        var body = Encoding.UTF8.GetBytes(jsonString);

                        // Create properties with metadata
                        var properties = channel.CreateBasicProperties();
                        properties.Persistent = true;
                        properties.ContentType = "application/json";  // Specify content type
                        properties.MessageId = order.OrderId;
                        properties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());

                        // Add custom headers
                        properties.Headers = new Dictionary<string, object>
                        {
                            { "order-type", "standard" },
                            { "priority", "normal" },
                            { "source", "web-application" }
                        };

                        // Publish to queue
                        channel.BasicPublish(
                            exchange: "",
                            routingKey: "orders",
                            basicProperties: properties,
                            body: body
                        );

                        Console.ForegroundColor = ConsoleColor.Blue;
                        Console.WriteLine($"📤 [Producer] Sent Order:");
                        Console.WriteLine($"   OrderId: {order.OrderId}");
                        Console.WriteLine($"   Customer: {order.Customer}");
                        Console.WriteLine($"   Items: {string.Join(", ", order.Items)}");
                        Console.WriteLine($"   Total: ${order.Total:F2}");
                        Console.WriteLine($"   JSON Size: {body.Length} bytes\n");
                        Console.ResetColor();

                        orderNumber++;
                        Thread.Sleep(5000);  // Send every 5 seconds
                    }
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"❌ [Producer] ERROR: {ex.Message}");
                Console.ResetColor();
            }
        }

        // Send a single order on demand
        static void SendSingleOrder()
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
                    channel.QueueDeclare(
                        queue: "orders",
                        durable: true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null
                    );

                    // Create a special manual order
                    var order = new OrderMessage
                    {
                        OrderId = $"MANUAL-{DateTime.Now.Ticks}",
                        Customer = "Manual Customer",
                        Items = new List<string> { "Express Item 1", "Express Item 2" },
                        Total = 299.99m,
                        OrderDate = DateTime.UtcNow,
                        Status = "Express"
                    };

                    string jsonString = JsonSerializer.Serialize(order, new JsonSerializerOptions
                    {
                        WriteIndented = true
                    });

                    var body = Encoding.UTF8.GetBytes(jsonString);

                    var properties = channel.CreateBasicProperties();
                    properties.Persistent = true;
                    properties.ContentType = "application/json";
                    properties.MessageId = order.OrderId;

                    channel.BasicPublish(
                        exchange: "",
                        routingKey: "orders",
                        basicProperties: properties,
                        body: body
                    );

                    Console.ForegroundColor = ConsoleColor.Magenta;
                    Console.WriteLine($"\n💬 [Manual] Sent Express Order: {order.OrderId} - ${order.Total:F2}\n");
                    Console.ResetColor();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ [Manual] ERROR: {ex.Message}");
            }
        }

        // Send multiple orders quickly
        static void SendOrderBurst(int count)
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
                    channel.QueueDeclare(
                        queue: "orders",
                        durable: true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null
                    );

                    Console.ForegroundColor = ConsoleColor.Magenta;
                    Console.WriteLine($"\n💥 [Burst] Sending {count} orders...\n");
                    Console.ResetColor();

                    for (int i = 1; i <= count; i++)
                    {
                        var order = new OrderMessage
                        {
                            OrderId = $"BURST-{i}",
                            Customer = $"Burst Customer {i}",
                            Items = new List<string> { $"Item {i}A", $"Item {i}B" },
                            Total = 50m + (i * 25m),
                            OrderDate = DateTime.UtcNow,
                            Status = "Bulk"
                        };

                        string jsonString = JsonSerializer.Serialize(order);
                        var body = Encoding.UTF8.GetBytes(jsonString);

                        var properties = channel.CreateBasicProperties();
                        properties.Persistent = true;
                        properties.ContentType = "application/json";

                        channel.BasicPublish(
                            exchange: "",
                            routingKey: "orders",
                            basicProperties: properties,
                            body: body
                        );

                        Console.ForegroundColor = ConsoleColor.Magenta;
                        Console.WriteLine($"   💥 Sent: {order.OrderId} - ${order.Total:F2}");
                        Console.ResetColor();

                        Thread.Sleep(200);
                    }

                    Console.ForegroundColor = ConsoleColor.Magenta;
                    Console.WriteLine($"\n✓ Burst complete!\n");
                    Console.ResetColor();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ [Burst] ERROR: {ex.Message}");
            }
        }

        // Consumer: Receives JSON messages and deserializes them to objects
        static void ConsumerWork()
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
                    // Declare the same queue
                    channel.QueueDeclare(
                        queue: "orders",
                        durable: true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null
                    );

                    // Fair dispatch
                    channel.BasicQos(
                        prefetchSize: 0,
                        prefetchCount: 1,
                        global: false
                    );

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"📬 [Consumer] Waiting for orders...\n");
                    Console.ResetColor();

                    var consumer = new EventingBasicConsumer(channel);

                    consumer.Received += (model, ea) =>
                    {
                        try
                        {
                            // Get message body
                            var body = ea.Body.ToArray();
                            var jsonString = Encoding.UTF8.GetString(body);

                            // Get message properties
                            var properties = ea.BasicProperties;

                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"📨 [Consumer] Received message:");
                            Console.WriteLine($"   Content-Type: {properties.ContentType}");
                            Console.WriteLine($"   Message-Id: {properties.MessageId}");

                            // Display custom headers if present
                            if (properties.Headers != null && properties.Headers.Count > 0)
                            {
                                Console.WriteLine($"   Headers:");
                                foreach (var header in properties.Headers)
                                {
                                    var value = Encoding.UTF8.GetString((byte[])header.Value);
                                    Console.WriteLine($"     - {header.Key}: {value}");
                                }
                            }

                            Console.WriteLine($"\n   Raw JSON:");
                            Console.WriteLine($"   {jsonString.Replace("\n", "\n   ")}\n");

                            // DESERIALIZE: Convert JSON string back to OrderMessage object
                            var order = JsonSerializer.Deserialize<OrderMessage>(jsonString);

                            if (order != null)
                            {
                                Console.WriteLine($"   Deserialized Object:");
                                Console.WriteLine($"   ✓ OrderId: {order.OrderId}");
                                Console.WriteLine($"   ✓ Customer: {order.Customer}");
                                Console.WriteLine($"   ✓ Items ({order.Items.Count}):");
                                foreach (var item in order.Items)
                                {
                                    Console.WriteLine($"       - {item}");
                                }
                                Console.WriteLine($"   ✓ Total: ${order.Total:F2}");
                                Console.WriteLine($"   ✓ Date: {order.OrderDate:yyyy-MM-dd HH:mm:ss} UTC");
                                Console.WriteLine($"   ✓ Status: {order.Status}");

                                // Simulate processing
                                Console.WriteLine($"\n   🔄 Processing order...");
                                Thread.Sleep(1000);

                                Console.WriteLine($"   ✅ Order {order.OrderId} processed successfully!\n");
                            }

                            Console.ResetColor();

                            // Manually acknowledge message
                            channel.BasicAck(ea.DeliveryTag, false);
                        }
                        catch (JsonException ex)
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine($"   ❌ [Consumer] JSON Deserialization Error: {ex.Message}");
                            Console.ResetColor();

                            // Reject message (don't requeue invalid JSON)
                            channel.BasicNack(ea.DeliveryTag, false, false);
                        }
                        catch (Exception ex)
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine($"   ❌ [Consumer] Processing Error: {ex.Message}");
                            Console.ResetColor();

                            // Reject and requeue for retry
                            channel.BasicNack(ea.DeliveryTag, false, true);
                        }
                    };

                    // Start consuming with manual acknowledgment
                    channel.BasicConsume(
                        queue: "orders",
                        autoAck: false,  // Manual acknowledgment for reliability
                        consumer: consumer
                    );

                    // Keep consumer alive
                    while (keepRunning)
                    {
                        Thread.Sleep(1000);
                    }

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"🛑 [Consumer] Shutting down...");
                    Console.ResetColor();
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"❌ [Consumer] ERROR: {ex.Message}");
                Console.ResetColor();
            }
        }

    }
}
