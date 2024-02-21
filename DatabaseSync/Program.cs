// Database logic
using System.Data;

string connectionStringMSSQL = "Server=localhost,1433;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;";
string connectionStringPostgres = "Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;Database=postgres_sync_database;";
var dbHelper = new DbHelper(connectionStringMSSQL);
var dbHelperPostgres = new DbHelperPostgresql(connectionStringPostgres);
dbHelperPostgres.checkIfDatabaseExists(); // Check if the database exists and create it if not
dbHelper.CreateLogsTable(); // Create the Logs table if not exists

// Console.WriteLine("Inserting sample log data...");
// for (int i = 2; i <= 11; i++)
// {
//     string sampleMonth = "March";
//     string sampleLogData = $"{{\"message\":\"Log entry {i}\",\"severity\":\"info\"}}";
//     dbHelper.InsertLogData(sampleMonth, sampleLogData);
// }

// Console.WriteLine("Sample log data inserted.");

var dataChanges = DbHelper.QueryCDCTables(connectionStringMSSQL);
Console.WriteLine("Data changes:");
foreach (DataTable table in dataChanges.Tables)
{
    Console.WriteLine($"Table: {table.TableName}");
    foreach (DataRow row in table.Rows)
    {
        Console.WriteLine(string.Join(", ", row.ItemArray));
    }
}