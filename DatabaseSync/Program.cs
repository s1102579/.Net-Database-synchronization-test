// Database logic
using System.Data;

string connectionStringMSSQL = "Server=localhost,1433;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;";
string connectionStringPostgres = "Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;Database=postgres_sync_database;";
var dbHelper = new DbHelper(connectionStringMSSQL);
var dbHelperPostgres = new DbHelperPostgresql(connectionStringPostgres);

// dbHelperPostgres.removeDatabaseIfExistandCreateANewOneWithSameName(); // Remove the database if exists and create a new one with the same name

// dbHelper.deleteDatabaseAndCreateNewOne(); // Delete the database and create a new one

// dbHelperPostgres.checkIfDatabaseExists(); // Check if the database exists and create it if not
// dbHelper.CreateLogsTable(); // Create the Logs table if not exists


// // run 1: insert Log data
// Console.WriteLine("Inserting sample log data...");
// for (int i = 2; i <= 11; i++)
// {
//     string sampleMonth = "March";
//     string sampleLogData = $"{{\"message\":\"Log entry {i}\",\"severity\":\"info\"}}";
//     dbHelper.InsertLogData(sampleMonth, sampleLogData);
// }

// Console.WriteLine("Sample log data inserted.");


// run 2: check cdc table and apply changes to postgres
var dataChanges = DbHelper.QueryCDCTables(connectionStringMSSQL);
Console.WriteLine("Data changes:");
foreach (DataTable table in dataChanges.Tables)
{
    table.TableName = "dbo.Logs"; // TODO temporary hardcoded, find out why the table name is "Table" and not "dbo.Logs
    Console.WriteLine($"Table: {table.TableName}");

    // Print column names
    Console.WriteLine("Columns: " + string.Join(", ", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName)));

    foreach (DataRow row in table.Rows)
    {
        Console.WriteLine(string.Join(", ", row.ItemArray));
    }
}

DbHelperPostgresql.ApplyChangesToPostgreSQL(dataChanges, connectionStringPostgres); // Apply the changes to the PostgreSQL database
