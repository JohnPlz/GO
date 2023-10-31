using MQTTnet.Server;
using MQTTnet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GO.Workerservice.Connection.Broker
{
    public class MBroker
    {
        private readonly MqttServer _broker;
        private readonly MqttFactory _mqttFactory;
        private readonly ILogger<MBroker> _logger;
        private readonly IConfiguration _configuration;

        public MBroker(ILogger<MBroker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            _mqttFactory = new MqttFactory();

            var options = GetOptions();

            _broker = _mqttFactory.CreateMqttServer(options);

            _broker.ClientConnectedAsync += ClientConnectedAsync;
            _broker.ClientDisconnectedAsync += ClientDisconnectedAsync;
        }

        private Task ClientDisconnectedAsync(ClientDisconnectedEventArgs e)
        {
            _logger.LogInformation($"The client: {e.ClientId} has been disconnected.");
            return Task.CompletedTask;
        }

        private Task ClientConnectedAsync(ClientConnectedEventArgs e)
        {
            _logger.LogInformation($"The client: {e.ClientId} has been connected.");
            return Task.CompletedTask;
        }

        private MqttServerOptions GetOptions()
        {
            var options = _mqttFactory.CreateServerOptionsBuilder()
                .WithDefaultEndpointPort(int.Parse(_configuration["BrokerPort"]))
                .WithDefaultEndpoint()
                .Build();

            return options;
        }

        public async Task RunBrokerAsync()
        {
            await _broker.StartAsync();
            _logger.LogInformation($"The broker has been startet successfully");
        }
    }
}
