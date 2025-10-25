using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ_Demo
{
    internal class SimpleQueueDemo
    {
        internal void PerformDemo()
        {
            Console.WriteLine("=== Simple RabbitMQ Producer-Consumer Demo ===\n");

            // Create two threads
            Thread producerThread = new Thread(ProducerWork);
            Thread consumerThread = new Thread(ConsumerWork);

            // Name threads for easier debugging
            producerThread.Name = "Producer Thread";
            consumerThread.Name = "Consumer Thread";

            // Start consumer first (so it's ready to receive)
            consumerThread.Start();

            // Small delay to ensure consumer is ready
            Thread.Sleep(1000);

            // Start producer
            producerThread.Start();

            // Wait for both threads to complete
            producerThread.Join();
            consumerThread.Join();

            Console.WriteLine("\n=== Program Complete ===");
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        // Producer thread method
        static void ProducerWork()
        {
            Console.WriteLine($"[{Thread.CurrentThread.Name}] Starting...\n");

            // Create connection factory
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };

            try
            {
                // Create connection and channel
                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    // Declare queue
                    channel.QueueDeclare(
                        queue: "hello_queue",
                        durable: false,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null
                    );

                    // Wait 2 seconds before sending
                    Console.WriteLine($"[{Thread.CurrentThread.Name}] Preparing to send message...");
                    Thread.Sleep(2000);

                    // Send message
                    string message = "Hello from Producer Thread!";
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(
                        exchange: "",
                        routingKey: "hello_queue",
                        basicProperties: null,
                        body: body
                    );

                    Console.WriteLine($"[{Thread.CurrentThread.Name}] ✓ SENT: '{message}'\n");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{Thread.CurrentThread.Name}] ERROR: {ex.Message}");
            }
        }

        // Consumer thread method
        static void ConsumerWork()
        {
            Console.WriteLine($"[{Thread.CurrentThread.Name}] Starting...\n");

            // Create connection factory
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };

            try
            {
                // Create connection and channel
                using (var connection = factory.CreateConnection())
                using (var channel = connection.CreateModel())
                {
                    // Declare the same queue
                    channel.QueueDeclare(
                        queue: "hello_queue",
                        durable: false,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null
                    );

                    Console.WriteLine($"[{Thread.CurrentThread.Name}] Waiting for messages...\n");

                    // Create consumer
                    var consumer = new EventingBasicConsumer(channel);

                    // Track if message received
                    bool messageReceived = false;

                    // Handle received messages
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);

                        Console.WriteLine($"[{Thread.CurrentThread.Name}] ✓ RECEIVED: '{message}'\n");
                        messageReceived = true;
                    };

                    // Start consuming
                    channel.BasicConsume(
                        queue: "hello_queue",
                        autoAck: true,
                        consumer: consumer
                    );

                    // Wait for message (max 10 seconds)
                    int waitTime = 0;
                    while (!messageReceived && waitTime < 10000)
                    {
                        Thread.Sleep(100);
                        waitTime += 100;
                    }

                    if (!messageReceived)
                    {
                        Console.WriteLine($"[{Thread.CurrentThread.Name}] Timeout - no message received");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{Thread.CurrentThread.Name}] ERROR: {ex.Message}");
            }
        }
    }
}
