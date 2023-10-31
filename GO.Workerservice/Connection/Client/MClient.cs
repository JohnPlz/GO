using MQTTnet.Client;
using MQTTnet.Packets;
using MQTTnet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace GO.Workerservice.Connection.Client
{
    public class MClient
    {
        public event EventHandler<string> OnReceivingMessage;
        public event EventHandler ClientConnected;
        public event EventHandler ClientDisconnected;

        public string BrokerIPAddress { get; private set; }
        public bool IsConnected { get; private set; }

        private readonly string _clientId;        
        private readonly List<string> _topics;

        private readonly IConfiguration _configuration;
        private readonly ILogger<MClient> _logger;

        private readonly IMqttClient _client;
        private readonly MqttFactory _mqttFactory;

        public MClient(ILogger<MClient> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;

            _clientId = $"{Guid.NewGuid()}";
            _topics = new()
            {
                configuration["MessageTopic"] ?? "go-service"
            };

            _mqttFactory = new MqttFactory();
            _client = _mqttFactory.CreateMqttClient();
            _client.ConnectedAsync += Connected;
            _client.DisconnectedAsync += Disconnected;
            _client.ApplicationMessageReceivedAsync += MessageReceived;
        }

        private Task MessageReceived(MqttApplicationMessageReceivedEventArgs msg) //TODO: Bestätigung ergänzen? => msg.AcknowledgeAsync()
        {
            if (msg is null || msg.ApplicationMessage is null)
            {
                return Task.CompletedTask;
            }
            else
            {
                var buffer = msg.ApplicationMessage.PayloadSegment.Array;

                if (buffer != null)
                {
                    string packet = Encoding.UTF8.GetString(buffer);

                    OnReceivingMessage?.Invoke(this, packet);
                }

                return Task.CompletedTask;
            }
        }

        private async Task Disconnected(MqttClientDisconnectedEventArgs arg)
        {
            if (arg.ClientWasConnected)
            {
                IsConnected = _client.IsConnected;

                ClientDisconnected?.Invoke(this, arg);
            }
        }

        private Task Connected(MqttClientConnectedEventArgs arg)
        {
            _logger.LogInformation($"The client {_clientId} has been connected to broker: {BrokerIPAddress}:{_configuration["BrokerPort"]}");
            ClientConnected?.Invoke(this, EventArgs.Empty);

            IsConnected = _client.IsConnected;

            return Task.CompletedTask;
        }

        public async Task Connect(CancellationToken cancellationToken)
        {
            try
            {
                var ips = GetInterNetworkIPAddresses().Select(x => x.MapToIPv4().ToString()).ToArray();
                string[] ipAddresses = new string[0];

                Array.Resize(ref ipAddresses, ips.Length + 1);
                Array.Copy(ips, 0, ipAddresses, 0, ips.Length);
                ipAddresses[ipAddresses.Length - 1] = _configuration["BrokerIPAddress"];

                for (int i = 0; i < ipAddresses.Length; i++)
                {
                    _logger.LogInformation($"Connection establishment to IP Address {ipAddresses[i]}:{_configuration["BrokerPort"]} is started...");

                    try
                    {
                        var options = GetOptions(ipAddresses[i]);
                        BrokerIPAddress = ipAddresses[i];

                        await _client.ConnectAsync(options, cancellationToken);
                        await SubscribeToTopics();

                        break;
                    }
                    catch (Exception exception)
                    {
                        _logger.LogWarning(exception.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex.ToString());
            }
        }

        private MqttClientOptions GetOptions(string ipAddress)
        {
            var options = _mqttFactory.CreateClientOptionsBuilder()
                .WithClientId(_clientId)
                .WithTcpServer(ipAddress, int.Parse(_configuration["BrokerPort"]))
                .Build();

            return options;
        }

        private IEnumerable<IPAddress>? GetInterNetworkIPAddresses()
        {
            var hostname = Dns.GetHostName();
            var addresses = Dns.GetHostEntry(hostname).AddressList
                .Where(x => x.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork);

            return addresses;
        }

        private async Task SubscribeToTopics() //TODO: to all topics from commondata / configuration
        {
            MqttClientSubscribeOptions? subscribeOptions;

            if (_topics.Count > 0)
            {
                subscribeOptions = _mqttFactory.CreateSubscribeOptionsBuilder()
                .Build();

                foreach (var topic in _topics)
                {
                    var filter = new MqttTopicFilter();
                    filter.Topic = topic;
                    subscribeOptions.TopicFilters.Add(filter);
                }

                await _client.SubscribeAsync(subscribeOptions);
            }
            
        }

    }
}
