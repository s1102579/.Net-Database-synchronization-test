// Database logic
string connectionString = "Server=localhost,1433;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;";
var dbHelper = new DbHelper(connectionString);
dbHelper.CreateLogsTable(); // Create the Logs table if not exists

Console.WriteLine("Inserting sample log data...");
for (int i = 2; i <= 11; i++)
{
    string sampleMonth = "January";
    string sampleLogData = $"{{\"message\":\"Log entry {i}\",\"severity\":\"info\"}}";
    dbHelper.InsertLogData(sampleMonth, sampleLogData);
}

Console.WriteLine("Sample log data inserted.");