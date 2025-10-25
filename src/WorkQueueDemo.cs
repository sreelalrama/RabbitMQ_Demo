using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ_Demo
{
    internal class WorkQueueDemo
    {
        internal void PerformDemo()
        {
            Console.WriteLine("  RabbitMQ Work Queue with Multiple Workers    ");
            Console.WriteLine("════════════════════════════════════════════════\n");

            // Create producer thread
            Thread producerThread = new Thread(ProducerWork);
            producerThread.Name = "Producer";

            // Create 3 worker threads
            Thread worker1Thread = new Thread(() => WorkerTask("Worker 1"));
            Thread worker2Thread = new Thread(() => WorkerTask("Worker 2"));
            Thread worker3Thread = new Thread(() => WorkerTask("Worker 3"));

            worker1Thread.Name = "Worker 1";
            worker2Thread.Name = "Worker 2";
            worker3Thread.Name = "Worker 3";

            // start all workers first (so they're ready to receive tasks)
            Console.WriteLine(" Starting Workers...\n");
            worker1Thread.Start();
            worker2Thread.Start();
            worker3Thread.Start();

            // wait a bit for workers to initialize
            Thread.Sleep(2000);

            // Start producer to send tasks
            Console.WriteLine(" Starting Producer...\n");
            producerThread.Start();

            // Wait for producer to finish
            producerThread.Join();

            Console.WriteLine("\n All tasks sent. Waiting for workers to finish...\n");

            // Give workers time to process all messages
            Thread.Sleep(15000);

            Console.WriteLine("             All Work Complete!                ");
            Console.WriteLine("════════════════════════════════════════════════\n");

            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }

        // Producer: Sends multiple tasks to the queue
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
                    // Declare a DURABLE queue (survives RabbitMQ restart)
                    channel.QueueDeclare(
                        queue: "task_queue",
                        durable: true,           // Queue persists
                        exclusive: false,
                        autoDelete: false,
                        arguments: null
                    );

                    Console.WriteLine("[Producer] Sending 10 tasks to queue...\n");

                    // Send 10 tasks with varying difficulty (dots = seconds of work)
                    for (int i = 1; i <= 10; i++)
                    {
                        // More dots = more work time
                        int dots = (i % 4) + 1;
                        string message = $"Task {i}" + new string('.', dots);
                        var body = Encoding.UTF8.GetBytes(message);

                        // Mark message as persistent
                        var properties = channel.CreateBasicProperties();
                        properties.Persistent = true;

                        channel.BasicPublish(
                            exchange: "",
                            routingKey: "task_queue",
                            basicProperties: properties,
                            body: body
                        );

                        Console.WriteLine($"   [Producer] Sent: '{message}' ({dots} seconds of work)");
                        Thread.Sleep(300); // Small delay between sends
                    }

                    Console.WriteLine("\n [Producer] All tasks sent!\n");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($" [Producer] ERROR: {ex.Message}");
            }
        }

        // Worker: Processes tasks from the queue
        static void WorkerTask(string workerName)
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
                        queue: "task_queue",
                        durable: true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null
                    );

                    // FAIR DISPATCH: Only send 1 message at a time to each worker
                    // This ensures even distribution of work
                    channel.BasicQos(
                        prefetchSize: 0,
                        prefetchCount: 1,    // Only 1 unacked message at a time
                        global: false
                    );

                    Console.WriteLine($"   [{workerName}] Ready and waiting for tasks...");

                    var consumer = new EventingBasicConsumer(channel);

                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);

                        // Count dots to determine work time
                        int dots = message.Split('.').Length - 1;

                        Console.WriteLine($"    [{workerName}] Received: '{message}' - Starting work...");

                        // Simulate work (each dot = 1 second)
                        Thread.Sleep(dots * 1000);

                        Console.WriteLine($"    [{workerName}] Completed: '{message}'");

                        // MANUAL ACK: Tell RabbitMQ we successfully processed the message
                        // If worker crashes before this, message will be redelivered
                        channel.BasicAck(
                            deliveryTag: ea.DeliveryTag,
                            multiple: false
                        );
                    };

                    // Start consuming with MANUAL acknowledgment
                    channel.BasicConsume(
                        queue: "task_queue",
                        autoAck: false,      // Manual ack for reliability
                        consumer: consumer
                    );

                    // Keep worker running for 20 seconds
                    Thread.Sleep(20000);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($" [{workerName}] ERROR: {ex.Message}");
            }
        }
    }
}
