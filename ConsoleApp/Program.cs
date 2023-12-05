using Apache.Arrow;
using Mapster;
using Newtonsoft.Json;
using System.ComponentModel;
using System.Data;
using Apache.Arrow.Types;
using Apache.Arrow.C;
using ChoETL;


class Program
{
    static void Main()
    {
        MapsterConfig.Configure();

        string filePath = Path.Combine("C:", "Users", "LorenzoCarpinelli", "source", "repos", "ConsoleApp", "ConsoleApp", "file", "test.txt");
        string outputFilePath = Path.Combine("C:", "Users", "LorenzoCarpinelli", "source", "repos", "ConsoleApp", "ConsoleApp", "file", "test.csv");
        string jsonFilePath = Path.Combine("C:", "Users", "LorenzoCarpinelli", "source", "repos", "ConsoleApp", "ConsoleApp", "file", "test.json");
        string parquetFilePath = Path.Combine("C:", "Users", "LorenzoCarpinelli", "source", "repos", "ConsoleApp", "ConsoleApp", "file", "test.parquet");

        List<string[]> righe = ReadFile(filePath);

        WriteToCsvFile(righe, outputFilePath);
        WriteToJsonFile(righe, jsonFilePath);
        WriteToParquetFile(righe, parquetFilePath);
    }

    private static List<string[]> ReadFile(string filePath)
    {
        using (StreamReader reader = new StreamReader(filePath))
        {
            var righe = new List<string[]>();

            while (!reader.EndOfStream)
            {
                string riga = reader.ReadLine();
                string[] charsRiga = riga.Split(' ');
                righe.Add(charsRiga);
            }

            return righe;
        }
    }
    private static DataTable CreateDataTable(List<string[]> righe)
    {
        DataTable dt = new DataTable();

        dt.Columns.Add("ID", typeof(int));
        dt.Columns.Add("Name", typeof(string));

        Modello modello = new Modello();

        foreach (string[] riga in righe)
        {
            modello = riga.Adapt<Modello>();
            dt.Rows.Add(modello.Id, modello.Name);
        }

        return dt;
    }

    private static void WriteToJsonFile(List<string[]> righe, string jsonFilePath)
    {
        using StreamWriter writer = new(jsonFilePath);
        Modello modello = new Modello();

        foreach (string[] riga in righe)
        {
            modello = riga.Adapt<Modello>();
            string jsonString = JsonConvert.SerializeObject(modello, Formatting.Indented);
            writer.WriteLine(jsonString);
            Console.WriteLine(jsonString);
        }
    }
    private static void WriteToCsvFile(List<string[]> righe, string outputFilePath)
    {
        using StreamWriter writer = new(outputFilePath);
        foreach (string[] riga in righe)
        {
            string csvLine = string.Join(",", riga);
            writer.WriteLine(csvLine);
        }
    }
    private static void WriteToParquetFile(List<string[]> righe, string parquetFilePath)
    {
        DataTable dataTable = CreateDataTable(righe);

        using (var w = new ChoParquetWriter(parquetFilePath))
        {
            w.Write(dataTable);
        }
    }
}
