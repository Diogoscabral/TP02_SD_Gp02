using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Collections.Generic;
using System.Timers;
using System.Threading.Tasks;
using Grpc.Core;
using TP2_SD_Gp02;

namespace TP2_SD_Gp02.Src
{
    public class PreprocessingServiceImpl : PreprocessingService.PreprocessingServiceBase
    {
        public override Task<PreprocessResponse> PreprocessJsonToCsv(PreprocessRequest request, ServerCallContext context)
        {
            Console.WriteLine("Received gRPC request for JSON preprocessing.");

            var jsonLines = request.JsonData.Split(new[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);
            var csvLines = new List<string>();

            foreach (var line in jsonLines)
            {
                try
                {
                    var entry = JsonSerializer.Deserialize<JsonEntry>(line, new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    });

                    if (entry != null && entry.Data != null)
                    {
                        csvLines.Add($"{entry.WavyId},{entry.Data.SensorType},{entry.Data.Value},{entry.Data.Timestamp}");
                    }
                    else
                    {
                        Console.WriteLine("Entrada inválida ou incompleta no JSON.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Erro ao processar linha JSON: {ex.Message}");
                }
            }

            string csvData = string.Join("\n", csvLines);
            return Task.FromResult(new PreprocessResponse { CsvData = csvData });
        }
    }

    public class Server_PP
    {
        private static readonly object fileLock = new object();
        private static System.Timers.Timer timer;
        private Grpc.Core.Server grpcServer;

        public void Start()
        {
            StartGrpcServer();
            StartFileProcessingTimer();
            Console.WriteLine("Servidor de pré-processamento está a monitorizar ficheiro JSON...");

            Console.WriteLine("Pressione ENTER para encerrar o servidor...");
            Console.ReadLine();
            Stop();

        }

        private void StartGrpcServer()
        {
            try
            {
                grpcServer = new Grpc.Core.Server
                {
                    Services = { PreprocessingService.BindService(new PreprocessingServiceImpl()) },
                    Ports = { new ServerPort("localhost", 5001, ServerCredentials.Insecure) }
                };

                grpcServer.Start();
                Console.WriteLine("gRPC Server iniciado em localhost:5001");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao iniciar servidor gRPC: {ex.Message}");
                throw;
            }
        }

        private void StartFileProcessingTimer()
        {
            timer = new System.Timers.Timer(10000);
            timer.Elapsed += async (sender, e) => await CheckAndProcessJson();
            timer.AutoReset = true;
            timer.Enabled = true;
        }

        public static Task CheckAndProcessJson()
        {
            lock (fileLock)
            {
                string jsonFilePath = @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP2_SD_Gp02\Data\preprocessing_data.json";
                string csvFilePath = @"C:\Users\luisc\Desktop\UNI\3º ANO\2 semestre\SD\TP2_SD_Gp02\TP2_SD_Gp02\Data\agregacao.csv";

                if (!File.Exists(jsonFilePath))
                {
                    Console.WriteLine("Ficheiro JSON não encontrado.");
                    return Task.CompletedTask;
                }

                var jsonLines = File.ReadAllLines(jsonFilePath)
                                    .Where(l => !string.IsNullOrWhiteSpace(l))
                                    .ToList();

                if (jsonLines.Count == 0)
                {
                    Console.WriteLine("Nenhum dado para processar.");
                    return Task.CompletedTask;
                }

                Console.WriteLine($"Foram encontrados {jsonLines.Count} registos. A processar...");

                var csvLines = new List<string>();

                foreach (var line in jsonLines)
                {
                    try
                    {
                        var entry = JsonSerializer.Deserialize<JsonEntry>(line, new JsonSerializerOptions
                        {
                            PropertyNameCaseInsensitive = true
                        });

                        if (entry != null && entry.Data != null)
                        {
                            csvLines.Add($"{entry.WavyId},{entry.Data.SensorType},{entry.Data.Value},{entry.Data.Timestamp}");
                            UpdateWavyStatus(entry.WavyId, "Conectado");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Erro ao processar linha JSON: {ex.Message}");
                    }
                }

                File.AppendAllLines(csvFilePath, csvLines);
                File.WriteAllText(jsonFilePath, string.Empty);

                Console.WriteLine("Processamento completo. Ficheiro JSON limpo.");
                return Task.CompletedTask;
            }
        }


        private static void UpdateWavyStatus(string wavyId, string status)
        {
            Console.WriteLine($"Status do {wavyId} atualizado para {status}");
        }

        public void Stop()
        {
            timer?.Stop();
            timer?.Dispose();
            grpcServer?.ShutdownAsync().Wait();
        }
    }

    public class JsonEntry
    {
        public string WavyId { get; set; }
        public SensorData Data { get; set; }
    }

    public class SensorData
    {
        public string SensorType { get; set; }
        public float Value { get; set; }
        public string Timestamp { get; set; }
    }
}