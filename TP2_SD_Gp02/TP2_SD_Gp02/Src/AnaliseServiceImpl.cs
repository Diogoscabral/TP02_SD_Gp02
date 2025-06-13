using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Grpc.Core;
using TP2_SD_Gp02;

namespace TP2_SD_Gp02.Src
{
    public class AnaliseServiceImpl : AnaliseService.AnaliseServiceBase
    {
        public override Task<AnaliseResponse> AnalisarDados(AnaliseRequest request, ServerCallContext context)
        {
            string dados = request.Dados;
            string[] partes = dados.Split('\n');

            if (partes.Length < 4)
            {
                return Task.FromResult(new AnaliseResponse
                {
                    Resultado = "Formato inválido. Esperado: WAVY_ID\\nsensor\\nvalor\\ntimestamp"
                });
            }

            string sensor = char.ToUpper(partes[1].Trim()[0]) + partes[1].Trim().Substring(1).ToLower();
            string valorStr = partes[2].Trim();

            if (!float.TryParse(valorStr.Replace(',', '.'), System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out float valor))
            {
                return Task.FromResult(new AnaliseResponse
                {
                    Resultado = $"Valor inválido: '{valorStr}' não é numérico."
                });
            }

            var limites = new Dictionary<string, (float min, float max, string mensagem)>
    {
        { "Temperatura", (15f, 30f, "Temperatura dentro do intervalo ideal.") },
        { "Humidade", (30f, 60f, "Humidade equilibrada.") },
        { "Vento", (0f, 40f, "Vento dentro do intervalo normal.") },
        { "Pressão", (980f, 1050f, "Pressão atmosférica estável.") },
        { "Luminosidade", (100f, 1000f, "Luminosidade adequada.") },
        { "TemperaturaMar", (10f, 27f, "Temperatura do mar dentro do intervalo aceitável.") }
    };

            if (limites.TryGetValue(sensor, out var intervalo))
            {
                string estado = (valor >= intervalo.min && valor <= intervalo.max)
                    ? intervalo.mensagem
                    : "Valor fora do intervalo recomendado.";

                string resultado = $"Sensor: {sensor} → Valor: {valor:F2} → {estado}";
                return Task.FromResult(new AnaliseResponse { Resultado = resultado });
            }
            else
            {
                return Task.FromResult(new AnaliseResponse
                {
                    Resultado = $"Sensor '{sensor}' não reconhecido. Valor: {valor:F2}"
                });
            }
        }


    }
}
