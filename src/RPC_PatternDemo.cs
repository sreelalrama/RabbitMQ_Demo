using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ_Demo
{
    internal class RPC_PatternDemo
    {
        private static volatile bool keepRunning = true;

        internal void PerformDemo()
        {
            Console.WriteLine("╔═══════════════════════════════════════════════════╗");
            Console.WriteLine("║         RPC Pattern - Request/Response Demo       ║");
            Console.WriteLine("╚═══════════════════════════════════════════════════╝\n");

            Console.WriteLine(" How RPC Works:");
            Console.WriteLine("   1. Client sends request with CorrelationId");
            Console.WriteLine("   2. Client creates temp reply queue (ReplyTo)");
            Console.WriteLine("   3. Server processes request");
            Console.WriteLine("   4. Server sends response to ReplyTo queue");
            Console.WriteLine("   5. Client matches response by CorrelationId\n");

            // Start RPC Server first
            Thread serverThread = new Thread(RpcServerWork);
            serverThread.Name = "RPC Server";
            serverThread.IsBackground = true;

            Console.WriteLine(" Starting RPC Server...\n");
            serverThread.Start();

            Thread.Sleep(2000);

            // Start RPC Client
            Thread clientThread = new Thread(RpcClientWork);
            clientThread.Name = "RPC Client";
            clientThread.IsBackground = true;

            Console.WriteLine(" Starting RPC Client...\n");
            clientThread.Start();

            // Interactive controls
            Console.WriteLine("═══════════════════════════════════════════════════");
            Console.WriteLine("Press 'Q' to quit");
            Console.WriteLine("Press 'R' to send a single RPC request");
            Console.WriteLine("Press 'B' to send a burst of 5 requests");
            Console.WriteLine("═══════════════════════════════════════════════════\n");

            while (keepRunning)
            {
                if (Console.KeyAvailable)
                {
                    var key = Console.ReadKey(true).Key;

                    if (key == ConsoleKey.Q)
                    {
                        Console.WriteLine("\n Shutting down...");
                        keepRunning = false;
                    }
                    else if (key == ConsoleKey.R)
                    {
                        Thread request = new Thread(() => SendSingleRequest());
                        request.Start();
                    }
                    else if (key == ConsoleKey.B)
                    {
                        Thread burst = new Thread(() => SendRequestBurst(5));
                        burst.Start();
                    }
                }

                Thread.Sleep(100);
            }

            Thread.Sleep(2000);
            Console.WriteLine("\n Application stopped. Goodbye!");
        }

        // RPC Client: Sends requests and waits for responses
        static void RpcClientWork()
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
                    // Declare the request queue (where server listens)
                    channel.QueueDeclare(
                        queue: "rpc_queue",
                        durable: false,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null
                    );

                    // Create a TEMPORARY reply queue for this client
                    var replyQueueDeclare = channel.QueueDeclare(
                        queue: "",           // Auto-generate name
                        durable: false,
                        exclusive: true,     // Only for this connection
                        autoDelete: true,    // Delete when client disconnects
                        arguments: null
                    );

                    string replyQueueName = replyQueueDeclare.QueueName;

                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine($" [RPC Client] Created reply queue: {replyQueueName}\n");
                    Console.ResetColor();

                    // Dictionary to track pending requests
                    var callbackMapper = new ConcurrentDictionary<string, TaskCompletionSource<string>>();

                    // Set up consumer for reply queue
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        // Get correlation ID from response
                        var correlationId = ea.BasicProperties.CorrelationId;

                        // Find the matching request
                        if (callbackMapper.TryRemove(correlationId, out var tcs))
                        {
                            var body = ea.Body.ToArray();
                            var response = Encoding.UTF8.GetString(body);

                            Console.ForegroundColor = ConsoleColor.Cyan;
                            Console.WriteLine($" [RPC Client] Received response:");
                            Console.WriteLine($"   CorrelationId: {correlationId}");
                            Console.WriteLine($"   Response: {response}\n");
                            Console.ResetColor();

                            // Complete the task (unblocks the waiting request)
                            tcs.TrySetResult(response);
                        }
                    };

                    // Start consuming from reply queue
                    channel.BasicConsume(
                        queue: replyQueueName,
                        autoAck: true,
                        consumer: consumer
                    );

                    int requestNumber = 1;

                    while (keepRunning)
                    {
                        // Create request (asking for Fibonacci number)
                        int n = 5 + (requestNumber % 10);
                        string request = n.ToString();

                        // Generate unique correlation ID
                        string correlationId = Guid.NewGuid().ToString();

                        // Create request properties
                        var properties = channel.CreateBasicProperties();
                        properties.CorrelationId = correlationId;  // Track this request
                        properties.ReplyTo = replyQueueName;       // Where to send response

                        // Create TaskCompletionSource to wait for response
                        var tcs = new TaskCompletionSource<string>();
                        callbackMapper[correlationId] = tcs;

                        var body = Encoding.UTF8.GetBytes(request);

                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine($" [RPC Client] Sending request:");
                        Console.WriteLine($"   Request: Fib({n})");
                        Console.WriteLine($"   CorrelationId: {correlationId}");
                        Console.WriteLine($"   ReplyTo: {replyQueueName}");
                        Console.ResetColor();

                        // Send request
                        channel.BasicPublish(
                            exchange: "",
                            routingKey: "rpc_queue",
                            basicProperties: properties,
                            body: body
                        );

                        requestNumber++;
                        Thread.Sleep(4000);  // Send request every 4 seconds
                    }
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($" [RPC Client] ERROR: {ex.Message}");
                Console.ResetColor();
            }
        }

        // RPC Server: Receives requests, processes them, and sends responses
        static void RpcServerWork()
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
                    // Declare request queue
                    channel.QueueDeclare(
                        queue: "rpc_queue",
                        durable: false,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null
                    );

                    // Fair dispatch: process one request at a time
                    channel.BasicQos(
                        prefetchSize: 0,
                        prefetchCount: 1,
                        global: false
                    );

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($" [RPC Server] Waiting for RPC requests...\n");
                    Console.ResetColor();

                    var consumer = new EventingBasicConsumer(channel);

                    consumer.Received += (model, ea) =>
                    {
                        string response = string.Empty;

                        // Get request properties
                        var body = ea.Body.ToArray();
                        var props = ea.BasicProperties;
                        var correlationId = props.CorrelationId;
                        var replyTo = props.ReplyTo;

                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($" [RPC Server] Received request:");
                        Console.WriteLine($"   CorrelationId: {correlationId}");
                        Console.WriteLine($"   ReplyTo: {replyTo}");
                        Console.ResetColor();

                        try
                        {
                            // Parse request
                            var message = Encoding.UTF8.GetString(body);
                            int n = int.Parse(message);

                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"   Request: Calculate Fibonacci({n})");
                            Console.WriteLine($"    Processing...");
                            Console.ResetColor();

                            // Calculate Fibonacci (simulate work)
                            Thread.Sleep(1000);  // Simulate processing time
                            int result = Fibonacci(n);
                            response = result.ToString();

                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"    Result: {result}");
                            Console.ResetColor();
                        }
                        catch (Exception ex)
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine($"    Error: {ex.Message}");
                            Console.ResetColor();
                            response = "ERROR";
                        }

                        // Create reply properties with SAME correlation ID
                        var replyProps = channel.CreateBasicProperties();
                        replyProps.CorrelationId = correlationId;  // CRITICAL: Match request!

                        var responseBytes = Encoding.UTF8.GetBytes(response);

                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($" [RPC Server] Sending response:");
                        Console.WriteLine($"   Response: {response}");
                        Console.WriteLine($"   CorrelationId: {correlationId}");
                        Console.WriteLine($"   To Queue: {replyTo}\n");
                        Console.ResetColor();

                        // Send response to client's reply queue
                        channel.BasicPublish(
                            exchange: "",
                            routingKey: replyTo,        // Send to client's reply queue
                            basicProperties: replyProps, // Include correlation ID
                            body: responseBytes
                        );

                        // Acknowledge request
                        channel.BasicAck(
                            deliveryTag: ea.DeliveryTag,
                            multiple: false
                        );
                    };

                    // Start consuming requests
                    channel.BasicConsume(
                        queue: "rpc_queue",
                        autoAck: false,  // Manual acknowledgment
                        consumer: consumer
                    );

                    // Keep server alive
                    while (keepRunning)
                    {
                        Thread.Sleep(1000);
                    }

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($" [RPC Server] Shutting down...");
                    Console.ResetColor();
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($" [RPC Server] ERROR: {ex.Message}");
                Console.ResetColor();
            }
        }

        // Calculate Fibonacci number (recursive - simple but slow)
        static int Fibonacci(int n)
        {
            if (n <= 1) return n;
            return Fibonacci(n - 1) + Fibonacci(n - 2);
        }

        // Send a single RPC request
        static void SendSingleRequest()
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
                    // Create temporary reply queue
                    var replyQueue = channel.QueueDeclare();
                    string replyQueueName = replyQueue.QueueName;

                    var callbackMapper = new ConcurrentDictionary<string, TaskCompletionSource<string>>();

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var correlationId = ea.BasicProperties.CorrelationId;
                        if (callbackMapper.TryRemove(correlationId, out var tcs))
                        {
                            var body = ea.Body.ToArray();
                            var response = Encoding.UTF8.GetString(body);
                            tcs.TrySetResult(response);
                        }
                    };

                    channel.BasicConsume(replyQueueName, true, consumer);

                    // Send request
                    int n = new Random().Next(10, 20);
                    string correlationId = Guid.NewGuid().ToString();

                    var properties = channel.CreateBasicProperties();
                    properties.CorrelationId = correlationId;
                    properties.ReplyTo = replyQueueName;

                    var tcs = new TaskCompletionSource<string>();
                    callbackMapper[correlationId] = tcs;

                    var body = Encoding.UTF8.GetBytes(n.ToString());

                    channel.BasicPublish("", "rpc_queue", properties, body);

                    Console.ForegroundColor = ConsoleColor.Magenta;
                    Console.WriteLine($"\n [Manual] Sent RPC request: Fib({n})");
                    Console.WriteLine($"   Waiting for response...");
                    Console.ResetColor();

                    // Wait for response (with timeout)
                    if (tcs.Task.Wait(TimeSpan.FromSeconds(10)))
                    {
                        Console.ForegroundColor = ConsoleColor.Magenta;
                        Console.WriteLine($"    Response: {tcs.Task.Result}\n");
                        Console.ResetColor();
                    }
                    else
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"    Timeout - no response received\n");
                        Console.ResetColor();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($" [Manual] ERROR: {ex.Message}");
            }
        }

        // Send multiple RPC requests
        static void SendRequestBurst(int count)
        {
            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine($"\n [Burst] Sending {count} RPC requests...\n");
            Console.ResetColor();

            for (int i = 0; i < count; i++)
            {
                Thread request = new Thread(() => SendSingleRequest());
                request.Start();
                Thread.Sleep(500);
            }
        }
    }
}
