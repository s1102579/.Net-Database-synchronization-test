// Database logic
using System.Data;
using DatabaseSync.Entities;

// Prerequisites: MSSQL database needs to be set up with CDC enabled and have a table called Logs with columns Month and LogData
// Prerequisites: PostgreSQL database needs to be set up with a table called dbo.Logs with columns Month and LogData
// Prerequisites: bot databases need to start with the same values in the Logs table. preferably empty
string connectionStringMSSQL = "Server=localhost,1434;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;";
string connectionStringPostgres = "Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;Database=postgres_sync_database;TrustServerCertificate=True;";
// var dbHelper = new DbHelper(connectionStringMSSQL);
// var dbHelperPostgres = new DbHelperPostgresql(connectionStringPostgres);

// dbHelperPostgres.removeDatabaseIfExistandCreateANewOneWithSameName(); // Remove the database if exists and create a new one with the same name
// dbHelper.deleteDatabaseAndCreateNewOne(); // Delete the database and create a new one
// dbHelperPostgres.checkIfDatabaseExists(); // Check if the database exists and create it if not
// dbHelper.CreateLogsTable(); // Create the Logs table if not exists

// await dbHelper.EmptyDatabaseCDCTableDboLogsAsync(); // Empty the Logs table


// // run 1: insert Log data
// Console.WriteLine("Inserting sample log data...");
// for (int i = 2; i <= 11; i++)
// {
//     string sampleMonth = "March";
//     string sampleLogData = $"{{\"message\":\"Log entry {i}\",\"severity\":\"info\"}}";
//     dbHelper.InsertLogDataAsync(sampleMonth, sampleLogData);
// }

// Console.WriteLine("Sample log data inserted.");

// // wait for changes to be captured by the CDC
// Console.WriteLine("Waiting for changes to be captured by the CDC...");
// Thread.Sleep(5000);
// Console.WriteLine("Changes captured.");



// // run 2: check cdc table and apply changes to postgres
// var dataChanges = DbHelper.QueryCDCTables(connectionStringMSSQL);
// Console.WriteLine("Data changes:");
// foreach (DataTable table in dataChanges.Tables)
// {
//     Console.WriteLine($"Table: {table.TableName}");

//     // Print column names
//     Console.WriteLine("Columns: " + string.Join(", ", table.Columns.Cast<DataColumn>().Select(c => c.ColumnName)));

//     foreach (DataRow row in table.Rows)
//     {
//         Console.WriteLine(string.Join(", ", row.ItemArray));
//     }
// }

// DbHelperPostgresql.ApplyChangesToPostgreSQL(dataChanges, connectionStringPostgres); // Apply the changes to the PostgreSQL database

//check sqlServer connection
// using (var context = SqlServerDbContext.Instance)
// {
//     try
//     {
//         // Try to query the database
//         var logs = context.AuditLogs.ToList();
//         Console.WriteLine("Connection to database successful.");
//     }
//     catch (Exception ex)
//     {
//         // If an exception is thrown, the connection failed
//         Console.WriteLine("Connection to database failed.");
//         Console.WriteLine("Error details: " + ex.Message);
//     }
// }

// DbHelper dbHelper = new DbHelper(SqlServerDbContext.Instance);
// await dbHelper.AddRowsToAuditLogTableWithCSVFileAsync("/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv");

// split data ip in multiple databases in Postgres
DbHelperPostgresql dbHelperPostgres = new DbHelperPostgresql(connectionStringPostgres, PostgreSqlDbContext.Instance);
List<AuditLog> logs = await dbHelperPostgres.GetAllDataFromAuditLogsTableAsync();
await dbHelperPostgres.DeleteAllDatabasesAsync();
await dbHelperPostgres.SplitDataUpInMultipleOwnDatabasesAsync(logs);
await dbHelperPostgres.InsertTaskGroupDataIntoDatabasesfromCsvFileAsync("/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/All_TaskGroups.csv");
// await dbHelperPostgres.InsertDataIntoDatabasesAsync(logs);

// // count the amount of unique JSON structures in the Log column
// DbHelper dbHelper = new DbHelper(SqlServerDbContext.Instance);
// HashSet<string> uniqueJsonStructures = await dbHelper.GetDistinctJsonStructuresAsync("/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv");
// var trulyUniqueJsonStructures = dbHelper.RemoveSubsets(uniqueJsonStructures);

// Console.WriteLine("Amount of unique JSON structures: " + uniqueJsonStructures.Count);
// Console.WriteLine("Amount of truly unique JSON structures: " + trulyUniqueJsonStructures.Count);

// foreach (var structure in trulyUniqueJsonStructures)
// {
//     Console.WriteLine();
//     Console.WriteLine(structure);
//     Console.WriteLine();
// }

// // find unique keys in Json file
// var uniqueKeys = await dbHelper.FindUniqueKeysAsync("/Users/timdekievit/Documents/Projects/Data-Sync-test/.Net-Database-synchronization-test/DatabaseSync/assets/AuditLogData.csv");
// Console.WriteLine("Amount of unique keys: " + uniqueKeys.Count);
// int tempCount = 0;
// foreach (var key in uniqueKeys)
// {
//     if (key.Contains('.'))
//     {
//         continue;
//     }
//     Console.WriteLine(key);
//     tempCount++;
//     // Console.WriteLine();
// }

// Console.WriteLine("Amount of unique keys without subkeys: " + tempCount);


// // check postgres connection
// using (var contextPSQL = PostgreSqlDbContext.Instance)
// {
//     try
//     {
//         // Try to query the database
//         var logs = contextPSQL.Logs.ToList();
//         Console.WriteLine("Connection to database successful.");
//     }
//     catch (Exception ex)
//     {
//         // If an exception is thrown, the connection failed
//         Console.WriteLine("Connection to database failed.");
//         Console.WriteLine("Error details: " + ex.Message);
//     }
// }


