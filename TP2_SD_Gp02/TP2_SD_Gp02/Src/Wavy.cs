using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace TP2_SD_Gp02.Src
{
    class Wavy
    {
        private readonly string id;
        private readonly string aggregatorIp;
        private readonly int port = 5000;
        private readonly Random random = new Random();
        private string currentState = "Conectado";
        private bool inMaintenance = false;
        private DateTime maintenanceEndTime;
        private readonly int maintenanceDuration = 20;
        private Task? autoSendTask = null;
        private CancellationTokenSource? autoSendTokenSource = null;
        private bool autoSending = false;

        public Wavy(string id, string aggregatorIp)
        {
            this.id = $"WAVY_{id}";
            this.aggregatorIp = aggregatorIp;
        }

        public Wavy(string id)
        {
            this.id = id;
        }

        private class SensorData
        {
            public string SensorType { get; set; }
            public float Value { get; set; }
            public string Timestamp { get; set; }
        }

        public void Start()
        {
            Console.WriteLine($"[{id}] WAVY iniciado. Pronto para receber comandos.");
            HandleUserCommands();
        }


        private void PublishToRabbitMQ(string sensorType, string data)
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
                using var connection = factory.CreateConnection();
                using var channel = connection.CreateModel();

                string exchangeName = "sensors";
                channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Topic, durable: false, autoDelete: false);

                string routingKey = $"sensor.{sensorType.ToLower()}";

                string messageWithId = $"{id}|{data}";
                var body = Encoding.UTF8.GetBytes(messageWithId);

                channel.BasicPublish(
                    exchange: exchangeName,
                    routingKey: routingKey,
                    basicProperties: null,
                    body: body
                );

                Console.WriteLine($"[{id}] Published to {routingKey}: {messageWithId}");
            }
            catch (RabbitMQ.Client.Exceptions.BrokerUnreachableException ex)
            {
                Console.WriteLine($"[{id}] ERRO: Não foi possível conectar ao RabbitMQ. Verifique se o serviço está em execução e acessível ({factory.HostName}:{factory.Port}).");
                Console.WriteLine($"Detalhes: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Inner: {ex.InnerException.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{id}] ERRO inesperado ao publicar no RabbitMQ: {ex.Message}");
                if (ex.InnerException != null)
                    Console.WriteLine($"Inner: {ex.InnerException.Message}");
            }
        }

        private void StartAutoSend(string sensorType, int intervalSeconds)
        {
            if (autoSending)
            {
                Console.WriteLine($"[{id}] Envio automático já está ativo.");
                return;
            }

            autoSendTokenSource = new CancellationTokenSource();
            var token = autoSendTokenSource.Token;

            autoSendTask = Task.Run(async () =>
            {
                autoSending = true;
                Console.WriteLine($"[{id}] Envio automático iniciado para '{sensorType}' a cada {intervalSeconds} segundos.");
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        if (!CheckMaintenance())
                        {
                            SendRandomData(sensorType);
                        }
                        await Task.Delay(intervalSeconds * 1000, token);
                    }
                    catch (TaskCanceledException)
                    {
                        
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[{id}] ERRO no envio automático: {ex.Message}");
                    }
                }
                autoSending = false;
            });
        }

        private void StopAutoSend()
        {
            if (!autoSending)
            {
                Console.WriteLine($"[{id}] Envio automático já está desligado.");
                return;
            }

            autoSendTokenSource?.Cancel();
            Console.WriteLine($"[{id}] Envio automático parado.");
        }



        private void SendRandomData(string sensorType)
        {
            try
            {
                if (CheckMaintenance())
                    return;

                if (currentState != "Conectado")
                {
                    Console.WriteLine($"[{id}] Estado atual é '{currentState}'. Envio de dados não permitido.");
                    return;
                }

                string filePath = GetFilePathBySensorType(sensorType);
                bool isJson = filePath.EndsWith(".json", StringComparison.OrdinalIgnoreCase);

                int wavyNumber = 0;
                string numericPart = id;
                if (id.StartsWith("WAVY_", StringComparison.OrdinalIgnoreCase))
                    numericPart = id.Substring(5);

                if (int.TryParse(numericPart, out wavyNumber))
                {
                    if (isJson && wavyNumber % 2 == 1)
                    {
                        Console.WriteLine($"[{id}] ERRO: WAVY com ID ímpar não pode enviar ficheiros JSON.");
                        return;
                    }
                    if (!isJson && wavyNumber % 2 == 0)
                    {
                        Console.WriteLine($"[{id}] ERRO: WAVY com ID par não pode enviar ficheiros CSV.");
                        return;
                    }
                }


                if (!File.Exists(filePath))
                {
                    Console.WriteLine($"[{id}] ERRO: Ficheiro não encontrado: {filePath}");
                    return;
                }

                string dataLine = isJson ? GetRandomJsonLine(filePath) : GetRandomCsvLine(filePath);

                if (string.IsNullOrEmpty(dataLine))
                {
                    Console.WriteLine($"[{id}] Nenhum dado disponível no arquivo {Path.GetExtension(filePath)}");
                    return;
                }

                PublishToRabbitMQ(sensorType, dataLine);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{id}] ERRO ao enviar dados: {ex.Message}");
            }
        }


        private string GetRandomJsonLine(string filePath)
        {
            try
            {
                var jsonData = System.Text.Json.JsonSerializer.Deserialize<List<SensorData>>(File.ReadAllText(filePath));
                if (jsonData == null || jsonData.Count == 0)
                {
                    Console.WriteLine("[AVISO] Arquivo JSON não contém dados válidos");
                    return string.Empty;
                }

                var randomEntry = jsonData[random.Next(jsonData.Count)];
                return System.Text.Json.JsonSerializer.Serialize(randomEntry);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{id}] ERRO ao ler JSON: {ex.Message}");
                return string.Empty;
            }
        }

        private void HandleUserCommands()
        {
            Console.WriteLine("\nComandos disponíveis:");
            Console.WriteLine("  DATA <tipo>           - Envia um dado aleatório do tipo especificado (temperatura, humidade, luminosidade)");
            Console.WriteLine("  STATUS                - Mostra o estado atual da ligação (e se está em manutenção)");
            Console.WriteLine("  MAINTENANCE           - Entra em modo de manutenção durante 20 segundos (bloqueia comandos)");
            Console.WriteLine("  AUTO ON <tipo> [seg]  - Começa envio automático de dados do tipo dado a cada 'seg' segundos (ex: AUTO ON temperatura 5)");
            Console.WriteLine("  AUTO OFF              - Para o envio automático de dados");
            Console.WriteLine("  QUIT                  - Termina a execução do programa\n");


            while (true)
            {
                CheckMaintenanceEnd();

                Console.Write("WAVY> ");
                var input = Console.ReadLine()?.Trim().ToUpper();

                if (input == null) continue;

                var parts = input.Split(' ');
                var command = parts[0];
                var argument = parts.Length > 1 ? parts[1] : string.Empty;

                switch (command)
                {
                    case "DATA":
                        if (CheckMaintenance())
                            break;

                        if (string.IsNullOrEmpty(argument))
                        {
                            Console.WriteLine("Especifique o tipo de sensor (humidade, temperatura, luminosidade)");
                        }
                        else
                        {
                            SendRandomData(argument);
                        }
                        break;
                    case "AUTO":
                        if (CheckMaintenance())
                            break;

                        if (parts.Length >= 3 && parts[1] == "ON")
                        {
                            string tipo = parts[2].ToLower();
                            int intervalo = 5;

                            if (parts.Length >= 4 && int.TryParse(parts[3], out int val))
                                intervalo = val;

                            StartAutoSend(tipo, intervalo);
                        }
                        else if (parts.Length >= 2 && parts[1] == "OFF")
                        {
                            StopAutoSend();
                        }
                        else
                        {
                            Console.WriteLine("Uso: AUTO ON <tipo> [intervalo]  |  AUTO OFF");
                        }
                        break;

                    case "STATUS":
                        if (CheckMaintenance())
                            break;

                        Console.WriteLine($"[{id}] Estado atual: {currentState}");
                        break;
                    case "MAINTENANCE":
                        StartMaintenance();
                        break;
                    case "QUIT":
                        if (CheckMaintenance())
                            break;

                        Console.WriteLine($"[{id}] Encerrando conexão...");
                        return;
                    default:
                        Console.WriteLine("Comando inválido. Use DATA <tipo>, STATUS, MAINTENANCE ou QUIT");
                        break;
                }
            }
        }


        private void StartMaintenance()
        {
            try
            {
                if (inMaintenance)
                {
                    Console.WriteLine($"[{id}] Já está em manutenção. Tempo restante: {(maintenanceEndTime - DateTime.Now).TotalSeconds:F0} segundos.");
                    return;
                }

               
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{id}] ERRO ao entrar em manutenção: {ex.Message}");
                inMaintenance = false;
            }
        }

        private bool CheckMaintenance()
        {
            if (inMaintenance)
            {
                TimeSpan remaining = maintenanceEndTime - DateTime.Now;
                if (remaining.TotalSeconds > 0)
                {
                    Console.WriteLine($"[{id}] Ação bloqueada. WAVY em manutenção por mais {remaining.TotalSeconds:F0} segundos.");
                    return true;
                }
                else
                {
                    CheckMaintenanceEnd();
                }
            }
            return false;
        }

        private void CheckMaintenanceEnd()
        {
            if (inMaintenance && DateTime.Now >= maintenanceEndTime)
            {
                inMaintenance = false;
                currentState = "Conectado";
                Console.WriteLine($"[{id}] Manutenção concluída. Estado voltou para Conectado.");
            }
        }

        private string GetRandomCsvLine(string filePath)
        {
            try
            {
                var lines = File.ReadAllLines(filePath)
                    .Skip(1) // Ignora o cabeçalho
                    .Where(line => !string.IsNullOrWhiteSpace(line))
                    .ToArray();

                if (lines.Length == 0)
                {
                    Console.WriteLine("[AVISO] Arquivo CSV não contém dados válidos");
                    return string.Empty;
                }

                var randomLine = lines[random.Next(lines.Length)];
                var parts = randomLine.Split(',');

                string value = parts[1].Trim();
                string timestampOriginal = parts[2].Trim();

                return $"{value},{timestampOriginal}";
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{id}] ERRO ao ler CSV: {ex.Message}");
                return string.Empty;
            }
        }


        private string GetFilePathBySensorType(string sensorType)
        {
            return sensorType.ToLower() switch
            {
                "humidade" => @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP2_SD_Gp02\Src\Data_Wavy\humidade.csv",
                "temperatura" => @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP2_SD_Gp02\Src\Data_Wavy\temperatura.csv",
                "luminosidade" => @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP2_SD_Gp02\Src\Data_Wavy\luminosidade.csv",
                "temperaturamar" => @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP2_SD_Gp02\Src\Data_Wavy\TemperaturaMar.json",
                "vento" => @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP2_SD_Gp02\Src\Data_Wavy\Vento.json",
                _ => throw new ArgumentException("Tipo de sensor inválido")
            };
        }

    }
}
