using CommandService.EventProcessing;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;

namespace CommandService.AsyncDataServices
{
    public class MessageBusSubscriber : BackgroundService
    {
        private readonly IConfiguration _configuration;
        private readonly IEventProcessor _eventProcessor;
        private readonly string _exchangeName = "trigger";
        private IConnection? _connection;
        private IChannel? _channel;
        private string? _queueName;


        public MessageBusSubscriber(IConfiguration configuration, IEventProcessor eventProcessor)
        {
            _configuration = configuration;
            _eventProcessor = eventProcessor;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            //stoppingToken.ThrowIfCancellationRequested();

            // 1. Initial Connection
            Console.WriteLine("--> MessageBusSubscriber: Starting up...");
            await InitializeRabbitMQ();
            if (_channel == null)
            {
                Console.WriteLine("--> MessageBusSubscriber: Failed to initialize. Check RabbitMQ connection.");
                return;
            }

            // 2. Setup Consumer
            var consumer = new AsyncEventingBasicConsumer(_channel);
            consumer.ReceivedAsync += async (ModuleHandle, ea) =>
            {
                Console.WriteLine("--> Event Received!");
                var body = ea.Body;
                var notificationMessage = Encoding.UTF8.GetString(body.ToArray());
                _eventProcessor.ProcessEvent(notificationMessage);
                await Task.CompletedTask;
            };

            // 3. Start Consuming
            await _channel.BasicConsumeAsync(queue: _queueName,
                autoAck: true,
                consumer: consumer,
                cancellationToken: stoppingToken);
            Console.WriteLine($"--> Listening on the Message Bus (Queue: {_queueName})...");

            // 4. The Heartbeat Loop
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    // Log a heartbeat every 30 seconds
                    Console.WriteLine($"--> Subscriber Heartbeat: {DateTime.Now:T} - Healthy");

                    // Wait for 30 seconds, or until the app shuts down
                    await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                // This is expected when the stoppingToken is triggered (App Shutdown)
                Console.WriteLine("--> MessageBusSubscriber: Stopping due to application shutdown.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"--> MessageBusSubscriber: Critical error in loop: {ex.Message}");
            }

        }
        private async Task InitializeRabbitMQ()
        {
            if (_channel != null && _channel.IsOpen) return;

            var factory = new ConnectionFactory()
            {
                HostName = _configuration["RabbitMQHost"],
                Port = int.Parse(_configuration["RabbitMQPort"])
            };

            try
            {
                _connection = await factory.CreateConnectionAsync();
                _channel = await _connection.CreateChannelAsync();

                // 1. Declare the exchange as Fanout (Broadcast)
                await _channel.ExchangeDeclareAsync(exchange: _exchangeName, type: ExchangeType.Fanout);

                // 2. Declare Queue (Must await the Task to get the result)
                var queueDeclareResult = await _channel.QueueDeclareAsync();
                _queueName = queueDeclareResult.QueueName;

                // 3. Bind Queue
                await _channel.QueueBindAsync(queue: _queueName,
                                             exchange: _exchangeName,
                                             routingKey: string.Empty);

                _connection.ConnectionShutdownAsync += RabbitMQ_ConnectionShutdown;
                Console.WriteLine($"--> Connected to Message Bus. Listening on Queue: {_queueName}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"--> Could not connect to the Message Bus: {ex.Message}");
            }
        }
        private Task RabbitMQ_ConnectionShutdown(object sender, ShutdownEventArgs e)
        {
            Console.WriteLine($"--> RabbitMQ Connection Shutdown {e.ReplyText}");
            return Task.CompletedTask;
        }
        public void Dispose()
        {
            Console.WriteLine("--> Message Bus Disposed");

            // 1. Check if the channel is open and close it
            if (_channel != null && _channel.IsOpen)
            {
                // In v7+, closing is async. 
                // In a synchronous Dispose, we use GetAwaiter().GetResult()
                _channel.CloseAsync().GetAwaiter().GetResult();
                _connection?.CloseAsync().GetAwaiter().GetResult();
            }

            // 2. THIS LINE fixes the warning
            GC.SuppressFinalize(this);
        }
    }
}
