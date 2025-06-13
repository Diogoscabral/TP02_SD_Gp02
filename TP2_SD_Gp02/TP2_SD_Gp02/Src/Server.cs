using System;
using System.Collections.Concurrent;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using TP2_SD_Gp02;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace TP2_SD_Gp02.Src
{
    class Server
    {
        private readonly TcpListener listener;
        private readonly int port = 6000;
        private readonly object fileLock = new();
        private readonly ConcurrentQueue<string> filaDeDados = new();
        private bool running = true;
        private bool notificacaoPendente = false;
        private bool reimprimirMenu = false;
        private bool menuAtivo = false;
        private object menuLock = new();




        private DateTime ultimaMensagemRecebida = DateTime.Now;

        public Server()
        {
            listener = new TcpListener(IPAddress.Any, port);
        }

        public void Start()
        {
            listener.Start();
            Console.WriteLine($"[SERVER] Iniciado na porta {port}");

            ultimaMensagemRecebida = DateTime.Now;

            Task.Run(() =>
            {
                while (running)
                {
                    var segundos = (DateTime.Now - ultimaMensagemRecebida).TotalSeconds;

                    if (segundos > 20)
                    {
                        lock (menuLock)
                        {
                            if (!menuAtivo)
                            {
                                Console.WriteLine("\n[INFO] Inatividade detetada. A mostrar menu principal automaticamente:");
                                Task.Run(() => MostrarMenuPrincipal());
                            }
                        }
                    }


                    Thread.Sleep(1000);
                }
            });


            Task.Run(() =>
            {
                DateTime ultimaConsulta = DateTime.MinValue;
                while (running)
                {
                    DateTime novaData = ObterUltimaDataResultado();
                    if (novaData > ultimaConsulta)
                    {
                        ultimaConsulta = novaData;
                        notificacaoPendente = true;
                    }
                    Thread.Sleep(20000);
                }
            });

            Task.Run(() =>
            {
                while (running)
                {
                    var client = listener.AcceptTcpClient();
                    new Thread(() => HandleClient(client)).Start();
                }
            });


            Console.WriteLine("[DEBUG] Entrou no if(reimprimirMenu), vai chamar MostrarMenuPrincipal()");

            MostrarMenuPrincipal();

        }
        private void HandleClient(TcpClient client)
        {
            using (client)
            using (var stream = client.GetStream())
            {
                stream.ReadTimeout = 2000;
                try
                {
                    while (true)
                    {
                        string message = ReceiveMessage(stream);
                        if (message == "__CLOSED__")
                            break;

                        if (string.IsNullOrEmpty(message))
                            continue;

                        Console.WriteLine($"[SERVER] Recebido: {message}");

                        if (message.StartsWith("CONNECT\n"))
                        {
                            SendResponse(stream, "100 OK");
                        }
                        else
                        {
                            string[] lines = message.Split(new[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);
                            foreach (var line in lines)
                            {
                                string sanitizedLine = SanitizeDecimalCommas(line);

                                lock (fileLock)
                                {
                                    string path = Path.Combine("Data", "Data_Server.csv");
                                    Directory.CreateDirectory("Data");
                                    File.AppendAllText(path, $"{DateTime.Now:yyyy-MM-dd HH:mm:ss},{sanitizedLine}\n");
                                }

                                string resultado = AnalisarDadosAsync(sanitizedLine).GetAwaiter().GetResult();
                                Console.WriteLine($"[SERVER] Resultado da análise: {resultado}");

                                var parts = sanitizedLine.Split(',');
                                if (parts.Length >= 5)
                                {
                                    string wavyID = parts[0];
                                    string tipoSensor = parts[1];
                                    string agregador = "Agregador1";
                                    DateTime dataHora = DateTime.Now;

                                    try
                                    {
                                        GarantirServidorExiste(wavyID, agregador);
                                        InserirResultadoAnalise(wavyID, resultado, dataHora, agregador, tipoSensor);
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"[SERVER] Erro ao guardar resultado na base de dados: {ex.Message}");
                                    }
                                }
                            }

                            SendResponse(stream, "100 OK");
                        }

                        ultimaMensagemRecebida = DateTime.Now;
                    }

                    ultimaMensagemRecebida = DateTime.Now;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[SERVER] ERRO: {ex.Message}");
                }
            }
        }

        private string SanitizeDecimalCommas(string input)
        {
            var parts = input.Split(',');
            if (parts.Length >= 3)
            {
                parts[2] = parts[2].Replace(',', '.');
                return string.Join(",", parts);
            }
            return input;
        }

        private string ReceiveMessage(NetworkStream stream)
        {
            byte[] buffer = new byte[1024];
            try
            {
                int bytesRead = stream.Read(buffer, 0, buffer.Length);
                if (bytesRead == 0)
                    return "__CLOSED__"; 
                return Encoding.UTF8.GetString(buffer, 0, bytesRead);
            }
            catch (IOException ex) when (ex.InnerException is SocketException se && se.SocketErrorCode == SocketError.TimedOut)
            {
                return null; 
            }
        }
        private void SendResponse(NetworkStream stream, string response)
        {
            byte[] data = Encoding.UTF8.GetBytes(response);
            stream.Write(data, 0, data.Length);
        }

        private async Task<string> AnalisarDadosAsync(string dados)
        {
            try
            {
                var parts = dados.Split(',');
                if (parts.Length < 5)
                    return "Formato inválido. Esperado: WAVY_ID,sensor,valor,unidade,timestamp";

                string timestamp = parts[4].Trim();
                string[] dataHora = timestamp.Split(' ');
                string data = dataHora.Length > 0 ? dataHora[0] : "0000-00-00";
                string hora = dataHora.Length > 1 ? dataHora[1] : "00:00:00";

                string formatted = $"{parts[0]}\n{parts[1]}\n{parts[2].Replace(',', '.')}\n{data} {hora}";

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
        private void InserirResultadoAnalise(string wavyID, string resultado, DateTime dataHora, string agregador, string tipoSensor)
        {
            string connectionString = @"Server=(localdb)\CABRAL;Database=SD_TP02;Trusted_Connection=True;";

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                try
                {
                    conn.Open();

                    string comandoSQL = @"INSERT INTO ResultadosAnalise (WavyID, Resultado, DataHoraAnalise, AgregadorAssociado, TipoSensor)
                                          VALUES (@WavyID, @Resultado, @DataHora, @Agregador, @TipoSensor)";

                    using (SqlCommand cmd = new SqlCommand(comandoSQL, conn))
                    {
                        cmd.Parameters.AddWithValue("@WavyID", wavyID);
                        cmd.Parameters.AddWithValue("@Resultado", resultado);
                        cmd.Parameters.AddWithValue("@DataHora", dataHora);
                        cmd.Parameters.AddWithValue("@Agregador", agregador);
                        cmd.Parameters.AddWithValue("@TipoSensor", tipoSensor);
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"[ERRO BD] Falha ao inserir resultado da análise: {ex.Message}");
                    Console.ResetColor();
                }
            }
        }
        private void GarantirServidorExiste(string wavyID, string agregador)
        {
            string connectionString = @"Server=(localdb)\CABRAL;Database=SD_TP02;Trusted_Connection=True;";
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();

                string checkQuery = "SELECT COUNT(*) FROM Servidor WHERE WavyID = @WavyID";
                using (SqlCommand checkCmd = new SqlCommand(checkQuery, conn))
                {
                    checkCmd.Parameters.AddWithValue("@WavyID", wavyID);
                    int count = (int)checkCmd.ExecuteScalar();

                    if (count == 0)
                    {
                        string insertQuery = "INSERT INTO Servidor (WavyID, DataUltimaSincronizacao, AgregadorAssociado) VALUES (@WavyID, @Data, @Agregador)";
                        using (SqlCommand insertCmd = new SqlCommand(insertQuery, conn))
                        {
                            insertCmd.Parameters.AddWithValue("@WavyID", wavyID);
                            insertCmd.Parameters.AddWithValue("@Data", DateTime.Now);
                            insertCmd.Parameters.AddWithValue("@Agregador", agregador);
                            insertCmd.ExecuteNonQuery();
                        }
                    }
                    else
                    {
                        string updateQuery = "UPDATE Servidor SET DataUltimaSincronizacao = @Data WHERE WavyID = @WavyID";
                        using (SqlCommand updateCmd = new SqlCommand(updateQuery, conn))
                        {
                            updateCmd.Parameters.AddWithValue("@Data", DateTime.Now);
                            updateCmd.Parameters.AddWithValue("@WavyID", wavyID);
                            updateCmd.ExecuteNonQuery();
                        }
                    }
                }
            }
        }


        private void MostrarMenuPrincipal()
        {
            lock (menuLock)
            {
                if (menuAtivo)
                    return;
                menuAtivo = true;
            }

            try
            {
                while (running)
                {
                    Console.WriteLine("\n[MENU PRINCIPAL]");
                    Console.WriteLine("1 - Estatísticas de Sensor");
                    Console.WriteLine("2 - Visualizar Resultados");
                    Console.WriteLine("0 - Sair");
                    Console.Write("Escolha: ");
                    string opcao = Console.ReadLine();

                    switch (opcao)
                    {
                        case "1":
                            MenuEstatisticasSensor();
                            break;
                        case "2":
                            MenuVisualizacaoResultados();
                            break;
                        case "0":
                            running = false;
                            return;
                        default:
                            Console.WriteLine("[ERRO] Opção inválida.");
                            break;
                    }

                    if (notificacaoPendente)
                    {
                        Console.WriteLine("\n[INFO] Novos resultados disponíveis na base de dados.");
                        Console.WriteLine("[INFO] Usa a opção 2 no menu principal para os visualizar.");
                        notificacaoPendente = false;
                    }

                    if (!running)
                        break;
                }
            }
            finally
            {
                lock (menuLock)
                {
                    menuAtivo = false;
                }
            }
        }

        private void MenuEstatisticasSensor()
        {
            while (true)
            {
                Console.WriteLine("\n[MENU ESTATÍSTICAS]");
                Console.WriteLine("0 - Voltar ao menu principal");
                Console.Write("Tipo de sensor (ex: vento): ");
                string tipoSensor = Console.ReadLine()?.Trim();

                if (tipoSensor == "0") break;

                Console.Write("Operação (MAX, MIN, MEDIA): ");
                string operacao = Console.ReadLine()?.Trim().ToUpper();

                if (string.IsNullOrEmpty(tipoSensor) || string.IsNullOrEmpty(operacao))
                {
                    Console.WriteLine("[ERRO] Entrada inválida.");
                    continue;
                }

                try
                {
                    double resultado = ObterEstatisticaSensor(tipoSensor, operacao);
                    Console.WriteLine($"[{operacao}] para sensor '{tipoSensor}': {resultado}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERRO] {ex.Message}");
                }
            }
        }


        private double ObterEstatisticaSensor(string tipoSensor, string operacao)
        {
            string funcaoSQL = operacao switch
            {
                "MAX" => "MAX",
                "MIN" => "MIN",
                "MEDIA" => "AVG",
                _ => throw new ArgumentException("Operação inválida. Use MAX, MIN ou MEDIA.")
            };

            string connectionString = @"Server=(localdb)\CABRAL;Database=SD_TP02;Trusted_Connection=True;";
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                string query = $@"
                SELECT {funcaoSQL}(TRY_CAST(
                    REPLACE(
                        LEFT(
                            LTRIM(
                                SUBSTRING(Resultado, CHARINDEX('Valor:', Resultado) + 6, 20)
                            ),
                            PATINDEX('%[^0-9,.]%', LTRIM(SUBSTRING(Resultado, CHARINDEX('Valor:', Resultado) + 6, 20)) + 'X') - 1
                        ),
                        ',', '.'
                    ) AS FLOAT)
                ) AS Estatistica
                FROM ResultadosAnalise
                WHERE TipoSensor = @TipoSensor";

                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    cmd.Parameters.AddWithValue("@TipoSensor", tipoSensor);
                    object result = cmd.ExecuteScalar();
                    return result != DBNull.Value ? Math.Round(Convert.ToDouble(result), 2) : double.NaN;
                }
            }
        }

        private void MenuVisualizacaoResultados()
        {
            while (true)
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("\n╔════════════════════════════════════════════════════╗");
                Console.WriteLine("║           VISUALIZAÇÃO DE RESULTADOS               ║");
                Console.WriteLine("╚════════════════════════════════════════════════════╝");
                Console.ResetColor();

                Console.Write("Tipo de sensor (ou ENTER para todos): ");
                string tipoSensor = Console.ReadLine()?.Trim();

                Console.Write("WavyID (ou ENTER para todos): ");
                string wavyID = Console.ReadLine()?.Trim();

                Console.Write("Data inicial (yyyy-MM-dd ou ENTER): ");
                string dataInicioStr = Console.ReadLine()?.Trim();
                DateTime? dataInicio = DateTime.TryParse(dataInicioStr, out var di) ? di : null;

                Console.Write("Data final (yyyy-MM-dd ou ENTER): ");
                string dataFimStr = Console.ReadLine()?.Trim();
                DateTime? dataFim = DateTime.TryParse(dataFimStr, out var df) ? df : null;

                if (!string.IsNullOrEmpty(dataInicioStr) && !dataInicio.HasValue)
                {
                    Console.WriteLine("[ERRO] Data inicial inválida.");
                    continue;
                }
                if (!string.IsNullOrEmpty(dataFimStr) && !dataFim.HasValue)
                {
                    Console.WriteLine("[ERRO] Data final inválida.");
                    continue;
                }

                string connectionString = @"Server=(localdb)\CABRAL;Database=SD_TP02;Trusted_Connection=True;";
                using (SqlConnection conn = new SqlConnection(connectionString))
                {
                    conn.Open();
                    string query = @"
                    SELECT WavyID, TipoSensor, Resultado, DataHoraAnalise
                    FROM ResultadosAnalise
                    WHERE (@TipoSensor IS NULL OR TipoSensor = @TipoSensor)
                      AND (@WavyID IS NULL OR WavyID = @WavyID)
                      AND (@DataInicio IS NULL OR DataHoraAnalise >= @DataInicio)
                      AND (@DataFim IS NULL OR DataHoraAnalise <= @DataFim)
                    ORDER BY DataHoraAnalise DESC";

                    using (SqlCommand cmd = new SqlCommand(query, conn))
                    {
                        cmd.Parameters.AddWithValue("@TipoSensor", string.IsNullOrEmpty(tipoSensor) ? (object)DBNull.Value : tipoSensor);
                        cmd.Parameters.AddWithValue("@WavyID", string.IsNullOrEmpty(wavyID) ? (object)DBNull.Value : wavyID);
                        cmd.Parameters.AddWithValue("@DataInicio", dataInicio.HasValue ? dataInicio : (object)DBNull.Value);
                        cmd.Parameters.AddWithValue("@DataFim", dataFim.HasValue ? dataFim : (object)DBNull.Value);

                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            Console.ForegroundColor = ConsoleColor.Yellow;
                            Console.WriteLine("╔════════╦═════════════════╦═══════════════════════════════════════════════════════════════════════════════════════════╦════════════════════╗");
                            Console.WriteLine("║ WavyID │ Sensor          │ Resultado                                                                                 │ DataHora           ║");
                            Console.WriteLine("╠════════╬═════════════════╬═══════════════════════════════════════════════════════════════════════════════════════════╬════════════════════╣");

                            Console.ResetColor();

                            bool temResultados = false;

                            while (reader.Read())
                            {
                                temResultados = true;

                                string wavy = reader["WavyID"].ToString().PadRight(7);
                                string sensor = reader["TipoSensor"].ToString().PadRight(16);
                                string resultado = reader["Resultado"].ToString();
                                if (resultado.Length > 90)
                                    resultado = resultado.Substring(0, 87) + "...";
                                resultado = resultado.PadRight(90);
                                string dataHora = Convert.ToDateTime(reader["DataHoraAnalise"]).ToString("yyyy-MM-dd HH:mm").PadRight(19);

                                Console.WriteLine($"║ {wavy}│ {sensor}│ {resultado}│ {dataHora}║");
                            }

                            if (!temResultados)
                            {
                                Console.ForegroundColor = ConsoleColor.Red;
                                Console.WriteLine("║ Nenhum resultado encontrado com os filtros aplicados.                             ║");
                                Console.ResetColor();
                            }

                            Console.ForegroundColor = ConsoleColor.Yellow;
                            Console.WriteLine("╚════════╩═════════════════╩═══════════════════════════════════════════════════════════════════════════════════════════╩════════════════════╝");

                            Console.ResetColor();
                        }
                    }
                }

                Console.Write("\nDeseja fazer nova consulta? (s/n): ");
                if (Console.ReadLine()?.Trim().ToLower() != "s")
                    break;
            }
        }

        private DateTime ObterUltimaDataResultado()
        {
            string connectionString = @"Server=(localdb)\CABRAL;Database=SD_TP02;Trusted_Connection=True;";
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                string query = "SELECT MAX(DataHoraAnalise) FROM ResultadosAnalise";
                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    object result = cmd.ExecuteScalar();
                    return result != DBNull.Value ? Convert.ToDateTime(result) : DateTime.MinValue;
                }
            }
        }


    }
}

