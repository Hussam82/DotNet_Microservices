using PlatformService.Dtos;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace PlatformService.AsyncDataServices
{
    public class MessageBusClient : IMessageBusClient, IDisposable
    {
        private readonly IConfiguration _configuration;
        private readonly string _exchangeName = "trigger";
        private IConnection? _connection;
        private IChannel? _channel;


        public MessageBusClient(IConfiguration configuration)
        {
            _configuration = configuration;
        }
       

        public async Task PublishNewPlatform(PlatformPublishedDto platformPublishedDto)
        {
            await InitializeRabbitMQ();

            var message = JsonSerializer.Serialize(platformPublishedDto);
            if(_connection.IsOpen)
            {
                Console.WriteLine("--> RabbitMQ Connection Open, sending message...");
                await SendMessage(message);
            }
            else
            {
                Console.WriteLine("--> RabbitMQ Connection is closed, not sending");
            }
        }

        private async Task SendMessage(string message)
        {
            var body = Encoding.UTF8.GetBytes(message);
            await _channel.BasicPublishAsync(
                exchange: _exchangeName,
                routingKey: string.Empty,
                body: body);
            Console.WriteLine($"--> We have sent {message}");
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

                // Declare the exchange as Fanout (Broadcast)
                await _channel.ExchangeDeclareAsync(exchange: _exchangeName, type: ExchangeType.Fanout);

                _connection.ConnectionShutdownAsync += RabbitMQ_ConnectionShutdown;

                Console.WriteLine("--> Connected to Message Bus");
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
