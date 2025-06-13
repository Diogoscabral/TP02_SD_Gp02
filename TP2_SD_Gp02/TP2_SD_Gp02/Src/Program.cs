using System;
using TP2_SD_Gp02.Src;

namespace TP2_SD_Gp02.Src
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Uso: dotnet run --project TP01_SD_testes <componente> [argumentos]");
                Console.WriteLine("Componentes disponíveis:");
                Console.WriteLine("  server");
                Console.WriteLine("  server_pp");
                Console.WriteLine("  server_analise");
                Console.WriteLine("  aggregator <id>");
                Console.WriteLine("  wavy <id>");
                return;
            }

            try
            {
                switch (args[0].ToLower())
                {
                    case "server":
                        StartServer();
                        break;
                    case "server_pp":
                        StartServerPP();
                        break;
                    case "server_analise":
                        StartServerAnalise();
                        break;
                    case "aggregator":
                        if (args.Length < 2) throw new ArgumentException("ID do Aggregator não especificado");
                        StartAggregator(args[1]);
                        break;
                    case "wavy":
                        if (args.Length < 2) throw new ArgumentException("ID da Wavy não especificado");
                        StartWavy(args[1]);
                        break;
                    default:
                        Console.WriteLine("Componente inválido");
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro: {ex.Message}");
            }
        }

        private static void StartServer()
        {
            var server = new Server();
            server.Start();
        }

        private static void StartServerPP()
        {
            var serverPP = new Server_PP();
            new Thread(() => NewMethod(serverPP)).Start();
            serverPP.Start();
        }

        private static void StartServerAnalise()
        {
            var serverAnalise = new Server_Analise();
            new Thread(() => serverAnalise.Start()).Start();
        }


        private static void NewMethod(Server_PP serverPP)
        {
            Task.Run(() => Server_PP.CheckAndProcessJson());
        }

        private static void StartAggregator(string id)
        {
            var aggregator = new Aggregator(id);
            aggregator.Start();
        }

        private static void StartWavy(string id)
        {
            var wavy = new Wavy(id);
            wavy.Start();
        }
    }
}