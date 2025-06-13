using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Grpc.Net.Client;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json;


namespace TP2_SD_Gp02.Src
{
    class Aggregator
    {
        private readonly string id;
        private readonly Dictionary<string, WavyConfig> wavyConfigs = new();    
        private readonly Dictionary<string, List<string>> dataBuffer = new();
        private TcpClient? serverClient;
        private NetworkStream? serverStream;
        private const int BufferLimit = 10;
        private readonly object fileLock = new();
        private System.Timers.Timer preprocessingTimer;
        private string sensorType = "all";

        private const int PreprocessingIntervalMs = 10000;

        public Aggregator(string id)
        {
            this.id = $"AGGREGATOR_{id}";
            InitializeConfigFiles();
        }

        public void Start()
        {
            Console.Write("Digite o tipo de sensor a subscrever (ex: vento, temperatura, all): ");
            var input = Console.ReadLine();
            sensorType = string.IsNullOrWhiteSpace(input) ? "all" : input.Trim();

            Console.WriteLine($"[{id}] Aggregator iniciado. A consumir mensagens do tipo: {sensorType}");

            try
            {
                serverClient = new TcpClient("localhost", 6000); 
                serverStream = serverClient.GetStream();
                SendToServer("CONNECT\n");
                string response = ReceiveServerResponse();
                Console.WriteLine($"[{id}] Ligação ao servidor: {response}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{id}] ERRO ao conectar ao servidor: {ex.Message}");
                return;
            }

            Task.Run(() => StartRabbitMqSubscriber());

            var agregacaoTimer = new System.Threading.Timer(TransferirAgregacaoParaEncaminhamento, null, 0, 5000);
            var envioTimer = new System.Threading.Timer(SendLinesToServer, null, 0, 5000);

            preprocessingTimer = new System.Timers.Timer(PreprocessingIntervalMs);
            preprocessingTimer.Elapsed += async (sender, e) => await AutoPreprocessJson();
            preprocessingTimer.AutoReset = true;
            preprocessingTimer.Enabled = true;

            HandleConsoleCommands();
        }


        private void InitializeConfigFiles()
        {
            try
            {
                string baseDir = Environment.CurrentDirectory;
                string configDir = Path.Combine(baseDir, "Config");

                if (!Directory.Exists(configDir))
                {
                    Directory.CreateDirectory(configDir);
                }

                string wavyConfigPath = Path.Combine(configDir, "wavy_config.csv");
                string prepConfigPath = Path.Combine(configDir, "preprocessing_config.csv");

                if (!File.Exists(wavyConfigPath))
                {
                    File.WriteAllText(wavyConfigPath, "WAVY_ID:status:[data_types]:last_sync\n");
                }

                if (!File.Exists(prepConfigPath))
                {
                    File.WriteAllText(prepConfigPath, "WAVY_ID:pré_processamento:volume_dados_enviar:servidor_associado\n");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{id}] ERRO CRÍTICO na inicialização: {ex}");
                Environment.Exit(1);
            }
        }

        private void StartRabbitMqSubscriber()
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                Port = 5672,
                UserName = "guest",
                Password = "guest"
            };

            try
            {
                var connection = factory.CreateConnection();
                var channel = connection.CreateModel();

                string exchangeName = "sensors";
                string queueName = $"aggregator_queue_{id}";
                string routingKey = sensorType.ToLower() == "all" ? "sensor.#" : $"sensor.{sensorType}";

                channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Topic, durable: false, autoDelete: false);
                channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false);
                channel.QueueBind(queue: queueName, exchange: exchangeName, routingKey: routingKey);


                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    try
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        var routing = ea.RoutingKey;

                        Console.WriteLine($"[{id}] Mensagem recebida: {routing} - {message}");

                        var parts = message.Split('|');
                        if (parts.Length >= 2)
                        {
                            string wavyId = parts[0].Trim();
                            string textData = parts[1].Trim();

                            string sensorType = routing.Replace("sensor.", "");

                            Console.WriteLine($"[{id}] Processando dados de {wavyId} - Tipo: {sensorType}");
                            HandleDataMessage(wavyId, textData, sensorType);
                        }
                        else
                        {
                            Console.WriteLine($"[{id}] AVISO: Mensagem com formato inválido recebida: {message}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[{id}] ERRO ao processar mensagem RabbitMQ: {ex.Message}");
                    }
                };

                channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);
                Console.WriteLine($"[{id}] Subscrito ao RabbitMQ com sucesso. Aguardando mensagens...");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{id}] ERRO CRÍTICO ao conectar ao RabbitMQ: {ex.Message}");
            }
        }

        private async Task AutoPreprocessJson()
        {
            string jsonFilePath = @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP2_SD_Gp02\Data\preprocessing_data.json";
            string csvOutputPath = @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP2_SD_Gp02\Data\encaminhamento.csv";

            string jsonData = null;

            lock (fileLock)
            {
                try
                {
                    if (!File.Exists(jsonFilePath)) return;

                    jsonData = File.ReadAllText(jsonFilePath);
                    if (string.IsNullOrWhiteSpace(jsonData)) return;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{id}] [gRPC] Erro no pré-processamento automático: {ex.Message}");
                    return;
                }
            }

            try
            {
                string csvData = await CallPreprocessingServiceAsync(jsonData);

                lock (fileLock)
                {
                    File.AppendAllText(csvOutputPath, csvData + Environment.NewLine);
                    File.WriteAllText(jsonFilePath, "");
                }

                Console.WriteLine($"[{id}] [gRPC] Pré-processamento automático concluído com sucesso.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{id}] [gRPC] Erro no pré-processamento automático: {ex.Message}");
            }
        }



        private async Task<string> CallPreprocessingServiceAsync(string jsonData)
        {
            try
            {
                using var channel = GrpcChannel.ForAddress("http://localhost:5001");
                var client = new PreprocessingService.PreprocessingServiceClient(channel);
                var request = new PreprocessRequest { JsonData = jsonData };
                var response = await client.PreprocessJsonToCsvAsync(request);
                return response.CsvData;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[gRPC] Erro ao conectar ao servidor de pré-processamento: {ex.Message}");
                return string.Empty;
            }
        }


        private void TransferirAgregacaoParaEncaminhamento(object? state)
        {
            lock (fileLock)
            {
                try
                {
                    string agregacaoPath = @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP2_SD_Gp02\Data\agregacao.csv";
                    string encaminhamentoPath = @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP2_SD_Gp02\Data\encaminhamento.csv";

                    if (!File.Exists(agregacaoPath)) return;

                    var linhas = File.ReadAllLines(agregacaoPath);
                    if (linhas.Length == 0) return;

                    File.AppendAllLines(encaminhamentoPath, linhas);
                    File.WriteAllText(agregacaoPath, string.Empty); 

                    Console.WriteLine($"[{id}] Dados transferidos de agregacao.csv para encaminhamento.csv.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{id}] ERRO ao transferir dados: {ex.Message}");
                }
            }
        }


        private void SendLinesToServer(object? state)
        {
            lock (fileLock)
            {
                try
                {
                    string encaminhamentoFilePath = @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP2_SD_Gp02\Data\encaminhamento.csv";
                    if (!File.Exists(encaminhamentoFilePath)) return;

                    var lines = File.ReadAllLines(encaminhamentoFilePath).ToList();
                    if (lines.Count >= 1)
                    {
                        var lineToSend = lines[0];
                        SendToServer($"{lineToSend}\n");
                        string response = ReceiveServerResponse();
                        if (response == "100 OK")
                        {
                            File.WriteAllLines(encaminhamentoFilePath, lines.Skip(1).ToList());
                        }
                        else
                        {
                            Console.WriteLine($"[{id}] ERRO ao enviar linha ao Server: {response}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{id}] ERRO ao enviar linha ao Server: {ex.Message}");
                }
            }
        }



        private void SetPreProcessingType(string wavyId, string preProcessingType)
        {
            lock (fileLock)
            {
                try
                {
                    string configPath = @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP1_SD_Gp02\Config\preprocessing_config.csv";

                    List<string> lines = File.Exists(configPath) ? File.ReadAllLines(configPath).ToList() : new List<string>();

                    if (lines.Count == 0 || !lines[0].StartsWith("WAVY_ID"))
                    {
                        lines.Insert(0, "WAVY_ID,pré_processamento,volume_dados_enviar,servidor_associado");
                    }

                    bool updated = false;
                    for (int i = 1; i < lines.Count; i++)
                    {
                        if (lines[i].StartsWith(wavyId + ","))
                        {
                            var parts = lines[i].Split(',');
                            lines[i] = $"{wavyId},{preProcessingType},{BufferLimit},default";
                            updated = true;
                            break;
                        }
                    }

                    if (!updated)
                    {
                        lines.Add($"{wavyId},{preProcessingType},{BufferLimit},default");
                    }

                    File.WriteAllLines(configPath, lines, Encoding.UTF8);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{id}] ERRO ao definir tipo de pré-processamento: {ex.Message}");
                }
            }
        }

        private void HandleDataMessage(string wavyId, string textData, string sensorType)
        {
            lock (fileLock)
            {
                try
                {
                    Console.WriteLine($"[{id}] Processando dados de {wavyId} - Sensor: {sensorType}");
                    UpdateWavyStatus(wavyId, "Operação");

                    bool isEvenId = int.TryParse(wavyId.Split('_').Last(), out int idNumber) && idNumber % 2 == 0;

                    if (isEvenId)
                    {
                        string dataFilePath = @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP2_SD_Gp02\Data\preprocessing_data.json";
                        var jsonData = new List<string>();

                        if (File.Exists(dataFilePath))
                            jsonData = File.ReadAllLines(dataFilePath).ToList();

                        var sensorData = JsonSerializer.Deserialize<SensorData>(textData);

                        var newEntry = new JsonEntry
                        {
                            WavyId = wavyId,
                            Data = sensorData
                        };

                        jsonData.Add(JsonSerializer.Serialize(newEntry));
                        File.WriteAllLines(dataFilePath, jsonData);

                        Console.WriteLine($"[{id}] Dados JSON de {wavyId} adicionados ao ficheiro de pré-processamento");
                    }
                    else
                    {
                        string csvPath = @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP2_SD_Gp02\Data\encaminhamento.csv";
                        string timestampAtual = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                        string line = $"{wavyId},{sensorType},{textData},{timestampAtual}";
                        File.AppendAllText(csvPath, line + Environment.NewLine);
                        Console.WriteLine($"[{id}] Dados CSV de {wavyId} adicionados ao ficheiro de encaminhamento");

                    }

                    if (!dataBuffer.ContainsKey(wavyId))
                        dataBuffer[wavyId] = new List<string>();

                    dataBuffer[wavyId].Add($"{sensorType}:{textData}");

                    if (dataBuffer[wavyId].Count >= BufferLimit)
                    {
                        Console.WriteLine($"[{id}] Buffer de {wavyId} atingiu o limite ({BufferLimit}). Processando...");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{id}] ERRO CRÍTICO no processamento de {wavyId}: {ex.Message}");
                }
            }
        }



        private void UpdateWavyStatus(string wavyId, string newStatus)
        {
            string statusPadronizado = newStatus switch
            {
                "Conectada" => "Conectado",
                "Desconectada" => "Desconectado",
                _ => newStatus
            };

            lock (fileLock)
            {
                try
                {
                    string configPath = @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP1_SD_Gp02\Config\wavy_config.csv";

                    Directory.CreateDirectory(Path.GetDirectoryName(configPath));

                    List<string> lines = File.Exists(configPath) ? File.ReadAllLines(configPath).ToList() : new List<string>();

                    if (lines.Count == 0 || !lines[0].StartsWith("WAVY_ID"))
                    {
                        lines.Insert(0, "WAVY_ID:status:[data_types]:last_sync");
                    }

                    bool isEvenId = int.TryParse(wavyId.Split('_').Last(), out int idNumber) && idNumber % 2 == 0;
                    string dataType = isEvenId ? "json" : "csv";

                    bool updated = false;
                    for (int i = 1; i < lines.Count; i++)
                    {
                        if (lines[i].StartsWith(wavyId + ":"))
                        {
                            var parts = lines[i].Split(':');
                            lines[i] = $"{wavyId}:{statusPadronizado}:{dataType}:{DateTime.Now:yyyy-MM-dd HH:mm:ss}";
                            updated = true;
                            break;
                        }
                    }

                    if (!updated)
                    {
                        lines.Add($"{wavyId}:{statusPadronizado}:[{dataType}]:{DateTime.Now:yyyy-MM-dd HH:mm:ss}");
                    }

                    File.WriteAllLines(configPath, lines, Encoding.UTF8);

                    if (wavyConfigs.ContainsKey(wavyId))
                    {
                        wavyConfigs[wavyId].Status = statusPadronizado;
                        wavyConfigs[wavyId].LastSync = DateTime.Now;
                    }

                    Console.WriteLine($"[{id}] Status atualizado: {wavyId} => {statusPadronizado}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{id}] ERRO ao atualizar status: {ex.Message}");
                }
            }
        }




        private async void HandleConsoleCommands()
        {
            while (true)
            {
                Console.WriteLine("\nComandos:");
                Console.WriteLine("  list - Listar Wavies");
                Console.WriteLine("  state <WavyId> <Estado> - Alterar estado");
                Console.WriteLine("  preprocess <WavyId> <Tipo> - Definir tipo de pré-processamento ");
                Console.WriteLine("  preprocessjson - Realizar pré-processamento de JSON para CSV");
                Console.WriteLine("  quit - Encerrar");

                Console.Write("aggregator> ");
                var input = Console.ReadLine()?.Trim().Split(' ');

                if (input == null || input.Length == 0) continue;

                try
                {
                    switch (input[0].ToLower())
                    {
                        case "list":
                            ListWavies();
                            break;
                        case "state" when input.Length == 3:
                            UpdateWavyStatus(input[1], input[2]);
                            break;
                        case "preprocess" when input.Length == 3:
                            SetPreProcessingType(input[1], input[2]);
                            break;
                        case "preprocessjson":
                            {
                                string jsonFilePath = @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP2_SD_Gp02\Data\preprocessing_data.json";
                                string csvOutputPath = @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP2_SD_Gp02\Data\encaminhamento.csv";

                                if (!File.Exists(jsonFilePath))
                                {
                                    Console.WriteLine("Ficheiro JSON não encontrado.");
                                    break;
                                }

                                string jsonData = File.ReadAllText(jsonFilePath);

                                try
                                {
                                    string csvData = await CallPreprocessingServiceAsync(jsonData);

                                    File.AppendAllText(csvOutputPath, csvData + Environment.NewLine);
                                    File.WriteAllText(jsonFilePath, ""); 

                                    Console.WriteLine("Pré-processamento concluído com sucesso!");
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"Erro ao chamar o servidor de pré-processamento: {ex.Message}");
                                }

                                break;
                            }

                        case "quit":
                            Environment.Exit(0);
                            break;
                        default:
                            Console.WriteLine("Comando inválido");
                            break;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Erro: {ex.Message}");
                }
            }
        }

        private void ListWavies()
        {
            lock (fileLock)
            {
                try
                {
                    string configPath = Path.GetFullPath(@"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP1_SD_Gp02\Config\wavy_config.csv");

                    if (!File.Exists(configPath))
                    {
                        Console.WriteLine("Arquivo de configuração não encontrado.");
                        return;
                    }

                    Console.WriteLine("\n=== WAVYs Registradas ===");
                    Console.WriteLine("ID".PadRight(15) + "Status".PadRight(12) + "Tipos de Dados".PadRight(20) + "Última Conexão");
                    Console.WriteLine(new string('-', 60));

                    foreach (var line in File.ReadLines(configPath).Skip(1))
                    {
                        var parts = line.Split(':');
                        if (parts.Length >= 4)
                        {
                            Console.WriteLine($"{parts[0].PadRight(15)}{parts[1].PadRight(12)}{parts[2].PadRight(20)}{parts[3]}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"\nERRO ao listar WAVYs: {ex.Message}");
                }
            }
        }

        private void SendToServer(string message)
        {
            try
            {
                byte[] data = Encoding.UTF8.GetBytes(message);
                serverStream.Write(data, 0, data.Length);
                Console.WriteLine($"[{id}] Dados enviados para o servidor: {message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{id}] ERRO ao enviar dados para o servidor: {ex.Message}");
            }
        }


        private string ReceiveServerResponse()
        {
            try
            {
                byte[] buffer = new byte[1024];
                int bytesRead = serverStream.Read(buffer, 0, buffer.Length);
                string response = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                Console.WriteLine($"[{id}] Resposta do servidor: {response}");
                return response;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{id}] ERRO ao receber resposta do servidor: {ex.Message}");
                return "500 ERRO";
            }
        }

    }

    class WavyConfig
    {
        public string Status { get; set; } = "Conectada ";
        public string[] DataTypes { get; set; } = Array.Empty<string>();
        public DateTime LastSync { get; set; } = DateTime.Now;
        public NetworkStream? Stream { get; set; }
    }

    class PreprocessingRecord
    {
        public string PreProcessing { get; set; } = "none";
        public int DataVolume { get; set; } = 10;
        public string TargetServer { get; set; } = "default";
    }
}



