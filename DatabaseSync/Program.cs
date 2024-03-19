// Database logic
using System.Data;
using DatabaseSync.Entities;

string connectionStringMSSQL = "Server=localhost,1434;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;";
string connectionStringPostgres = "Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;Database=postgres_sync_database;TrustServerCertificate=True;";

// add data to MSSQL
DbHelper dbHelperMSSQL = new DbHelper(SqlServerDbContext.Instance);
await dbHelperMSSQL.EmptyDatabaseTableDboAuditLogsAsync();
await dbHelperMSSQL.AddRowsToAuditLogTableWithCSVFileAsync("assets/AuditLogData.csv");
List<AuditLog> logs = await dbHelperMSSQL.GetDataFromAuditLogsTableAsync();

// split data ip in multiple databases in Postgres
DbHelperPostgresql dbHelperPostgres = new DbHelperPostgresql(connectionStringPostgres, PostgreSqlDbContext.Instance);
await dbHelperPostgres.DeleteAllDatabasesAsync();
await dbHelperPostgres.SplitDataUpInMultipleOwnDatabasesAsync(logs);
await dbHelperPostgres.InsertPUserDataIntoDatabasesfromCsvFileAsync("assets/PUsers_Pepperflow.csv"); // PUsers_Pepperflow.csv
await dbHelperPostgres.InsertTaskGroupDataIntoDatabasesfromCsvFileAsync("assets/AllTaskgroupsWithType_1AndIsNotDeleted.csv"); // AllTaskgroupsWithType_1AndIsNotDeleted.csv
await dbHelperPostgres.InsertAuditLogsIntoDatabaseAsync(logs);


