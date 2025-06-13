using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Net.Client;
using TP2_SD_Gp02;

namespace TP2_SD_Gp02.Src
{
    class Server_Analise
    {
        private readonly TcpListener listener;
        private readonly int port = 7001;
        private readonly object fileLock = new();
        private Grpc.Core.Server grpcServer;

        public Server_Analise()
        {
            listener = new TcpListener(IPAddress.Any, port);
        }

        public void Start()
        {
            StartGrpcService();

            listener.Start();
            Console.WriteLine($"[SERVER_ANALISE] Iniciado na porta {port}");

            while (true)
            {
                var client = listener.AcceptTcpClient();
                new Thread(() => HandleClient(client)).Start();
            }
        }

        private void StartGrpcService()
        {
            grpcServer = new Grpc.Core.Server
            {
                Services = { AnaliseService.BindService(new AnaliseServiceImpl()) },
                Ports = { new ServerPort("localhost", 7000, ServerCredentials.Insecure) }
            };

            grpcServer.Start();
            Console.WriteLine("[SERVER_ANALISE] Serviço gRPC de análise ativo em localhost:7000");
        }

        private void HandleClient(TcpClient client)
        {
            using (client)
            using (var stream = client.GetStream())
            {
                try
                {
                    while (true)
                    {
                        string message = ReceiveMessage(stream);
                        if (string.IsNullOrEmpty(message)) break;

                        Console.WriteLine($"[SERVER_ANALISE] Recebido: {message}");

                        if (message.StartsWith("CONNECT\n"))
                        {
                            SendResponse(stream, "100 OK");
                        }
                        else
                        {
                            string[] lines = message.Split(new[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);
                            foreach (var line in lines)
                            {
                                string resultado = AnalisarDadosAsync(line).Result;
                                Console.WriteLine($"[SERVER_ANALISE] Resultado da análise: {resultado}");
                            }
                            SendResponse(stream, "100 OK");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[SERVER_ANALISE] ERRO: {ex.Message}");
                }
            }
        }


        private async Task<string> AnalisarDadosAsync(string dados)
        {
            try
            {
                var parts = dados.Split(',');
                if (parts.Length < 5)
                    return "Formato inválido. Esperado: WAVY_ID\nsensor\nvalor\ntimestamp";

                string formatted = $"{parts[0]}\n{parts[1]}\n{parts[2]}\n{parts[4]} {parts[5]}";

                using var channel = GrpcChannel.ForAddress("http://localhost:7000");
                var client = new AnaliseService.AnaliseServiceClient(channel);
                var request = new AnaliseRequest { Dados = formatted };
                var response = await client.AnalisarDadosAsync(request);
                return response.Resultado;
            }
            catch (Exception ex)
            {
                return $"Erro na análise: {ex.Message}";
            }
        }


        private string ReceiveMessage(NetworkStream stream)
        {
            byte[] buffer = new byte[1024];
            int bytesRead = stream.Read(buffer, 0, buffer.Length);
            return Encoding.UTF8.GetString(buffer, 0, bytesRead);
        }

        private void SendResponse(NetworkStream stream, string response)
        {
            byte[] data = Encoding.UTF8.GetBytes(response);
            stream.Write(data, 0, data.Length);
        }

        public async Task StopAsync()
        {
            await grpcServer.ShutdownAsync();
        }
    }
}
