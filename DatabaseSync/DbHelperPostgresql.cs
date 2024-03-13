

using System.Data;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using DatabaseSync.Entities;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Npgsql.Replication;

public class DbHelperPostgresql
{
    private readonly string _connectionString;
    private readonly PostgreSqlDbContext _context;

    public DbHelperPostgresql(string connectionString, PostgreSqlDbContext context)
    {
        _context = context;
        this._connectionString = connectionString;
    }

    public async Task InsertListOfLogDataAsync(List<Log> logs)
    {
        _context.Logs.AddRange(logs);
        await _context.SaveChangesAsync();
    }

    public async Task InsertListOfAuditLogDataAsync(List<AuditLog> auditLogs) // uses binary import
    {
        using (var conn = new NpgsqlConnection("Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;Database=postgres_sync_database;TrustServerCertificate=True;"))
        {
            await conn.OpenAsync();

            using (var writer = conn.BeginBinaryImport("COPY \"AuditLog_20230101\" (\"AccountId\", \"PUser_Id\", \"ImpersonatedUser_Id\", \"Type\", \"Table\", \"Log\", \"Created\") FROM STDIN (FORMAT BINARY)"))
            {
                foreach (var auditLog in auditLogs)
                {
                    writer.StartRow();
                    writer.Write(auditLog.AccountId, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(auditLog.PUser_Id, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(auditLog.ImpersonatedUser_Id, NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(auditLog.Type, NpgsqlTypes.NpgsqlDbType.Smallint);
                    writer.Write(auditLog.Table, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(auditLog.Log, NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(auditLog.Created, NpgsqlTypes.NpgsqlDbType.Timestamp);
                }

                await writer.CompleteAsync();
            }
        }
    }

    public async Task InsertTaskGroupDataIntoDatabasesfromCsvFileAsync(string csvFilePath)
    {
        string connectionString = "Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;TrustServerCertificate=True;";

        DataTable taskGroups = await ReadTaskgroupCsvIntoDataTable(csvFilePath);

        // Sort and group the data based on Account_Id
        var groupedData = taskGroups.AsEnumerable().GroupBy(row => row["Account_Id"]);

        foreach (var group in groupedData)
        {
            await InsertTaskgroupsIntoDatabase(group, connectionString);
        }
    }

    public async Task EmptyDatabaseTableDboLogsAsync()
    {
        _context.Logs.RemoveRange(_context.Logs);
        await _context.SaveChangesAsync();
    }

    public async Task EmptyDatabaseTableAuditLogsAsync()
    {
        await _context.Database.ExecuteSqlRawAsync("DELETE FROM \"AuditLog_20230101\"");
    }

    public async Task<List<AuditLog>> GetAllDataFromAuditLogsTableAsync()
    {
        return await _context.AuditLogs.ToListAsync();
    }

    public async Task SplitDataUpInMultipleOwnDatabasesAsync(List<AuditLog> auditLogs)
    {
        string connectionString = "Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;TrustServerCertificate=True;";
        HashSet<int> processedAccountIds = new HashSet<int>();

        foreach (var auditLog in auditLogs)
        {
            if (auditLog.AccountId.HasValue && !processedAccountIds.Contains(auditLog.AccountId.Value))
            {
                processedAccountIds.Add(auditLog.AccountId.Value);
                await CreateDatabaseAndTablesIfNotExists(auditLog.AccountId.Value, connectionString);
            }
        }
    }

    public async Task DeleteAllDatabasesAsync() // Delete all databases method for testing purposes
    {
        string connectionString = "Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;TrustServerCertificate=True;";

        // Create a HashSet with values from 1 to 2000
        HashSet<int> processedAccountIds = new HashSet<int>(Enumerable.Range(1, 2000));

        foreach (var accountId in processedAccountIds)
        {
            await DeleteDatabaseIfItExists(accountId, connectionString);
        }
    }

    public async Task AddRowsToAuditLogTableWithCSVFileExceptForOneDayAsync(string path)
    {
        var auditLogs = await ReadAuditLogsFromCSV(path);
        await WriteAuditLogsToDatabase(auditLogs);
    }

    private async Task<DataTable> ReadAuditLogsFromCSV(string path)
    {
        var auditLogs = new DataTable();

        auditLogs.Columns.Add("AccountId", typeof(int));
        auditLogs.Columns.Add("PUser_Id", typeof(int));
        auditLogs.Columns.Add("ImpersonatedUser_Id", typeof(int));
        auditLogs.Columns.Add("Type", typeof(byte));
        auditLogs.Columns.Add("Table", typeof(string));
        auditLogs.Columns.Add("Log", typeof(string));
        auditLogs.Columns.Add("Created", typeof(DateTime));

        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = false, // Set to true if your CSV file has a header
            Delimiter = ",",
            BadDataFound = null
        };

        using (var reader = new StreamReader(path))
        using (var csv = new CsvReader(reader, config))
        {
            while (await csv.ReadAsync())
            {
                var created = csv.GetField(6);

                if (!created.Contains("2023-01-31"))
                {
                    var row = auditLogs.NewRow();
                    row["AccountId"] = ParseNullableInt(csv.GetField(0));
                    row["PUser_Id"] = ParseNullableInt(csv.GetField(1));
                    row["ImpersonatedUser_Id"] = ParseNullableInt(csv.GetField(2));
                    row["Type"] = byte.Parse(csv.GetField(3));
                    row["Table"] = csv.GetField(4);
                    row["Log"] = csv.GetField(5);
                    row["Created"] = DateTime.Parse(created);

                    auditLogs.Rows.Add(row);
                }
            }
        }

        return auditLogs;
    }

    private object ParseNullableInt(string value)
    {
        return value == "NULL" ? DBNull.Value : (object)int.Parse(value);
    }

    private async Task WriteAuditLogsToDatabase(DataTable auditLogs)
    {
        using (var conn = new NpgsqlConnection("Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;Database=postgres_sync_database;TrustServerCertificate=True;")) // hardcoded connection string for testing purposes
        {
            await conn.OpenAsync();

            using (var writer = conn.BeginBinaryImport("COPY \"AuditLog_20230101\" (\"AccountId\", \"PUser_Id\", \"ImpersonatedUser_Id\", \"Type\", \"Table\", \"Log\", \"Created\") FROM STDIN (FORMAT BINARY)"))
            {
                foreach (DataRow row in auditLogs.Rows)
                {
                    writer.StartRow();
                    writer.Write(row["AccountId"], NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(row["PUser_Id"], NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(row["ImpersonatedUser_Id"], NpgsqlTypes.NpgsqlDbType.Integer);
                    writer.Write(row["Type"], NpgsqlTypes.NpgsqlDbType.Smallint);
                    writer.Write(row["Table"], NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(row["Log"], NpgsqlTypes.NpgsqlDbType.Text);
                    writer.Write(row["Created"], NpgsqlTypes.NpgsqlDbType.Timestamp);
                }

                await writer.CompleteAsync();
            }
        }
    }

    private async Task CreateDatabaseAndTablesIfNotExists(int accountId, string connectionString)
    {
        string dbName = $"\"AuditLog_{accountId}\"";
        using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();

            if (!await DatabaseExists(dbName, connection))
            {
                await CreateDatabase(dbName, connection);
                await CreateTablesInDatabase(dbName, connectionString);
            }

            await connection.CloseAsync();
        }
    }

    private async Task<bool> DatabaseExists(string dbName, NpgsqlConnection connection)
    {
        var command = new NpgsqlCommand($"SELECT 1 FROM pg_database WHERE datname = '{dbName.Replace("\"", "")}'", connection);
        return await command.ExecuteScalarAsync() != null;
    }

    private async Task CreateDatabase(string dbName, NpgsqlConnection connection)
    {
        var command = new NpgsqlCommand($"CREATE DATABASE {dbName}", connection);
        await command.ExecuteNonQueryAsync();
    }

    private async Task CreateTablesInDatabase(string dbName, string connectionString)
    {
        var dbConnection = new NpgsqlConnection(connectionString + $"Database={dbName};");
        await dbConnection.OpenAsync();

        string createTableQuery = @"
            CREATE TABLE ""PUser"" (
                ""PUser_Id"" integer PRIMARY KEY,
                ""Firstname"" varchar(128),
                ""LastName"" varchar(128),
                ""Email"" varchar(128),
                ""Guid"" uuid
            );

            CREATE TABLE ""Operation"" (
                ""Operation_Id"" integer PRIMARY KEY,
                ""Type_Id"" integer,
                ""Data_Id"" integer,
                ""Created"" timestamp
            );

            CREATE TABLE ""OperationPUser"" (
                ""PUser_id"" integer,
                ""Operation_Id"" integer,
                PRIMARY KEY (""PUser_id"", ""Operation_Id"")
            );

            CREATE TABLE ""Data"" (
                ""Data_Id"" integer PRIMARY KEY,
                ""Table"" varchar(128),
                ""Data"" text
            );

            CREATE TABLE ""Type"" (
                ""Type_Id"" integer PRIMARY KEY,
                ""Name"" varchar(128)
            );

            CREATE TABLE ""TaskGroup"" (
                ""Taskgroup_Id"" integer PRIMARY KEY,
                ""Guid"" uuid,
                ""Name"" varchar(255),
                ""GlobalID"" text
            );";

        var command = new NpgsqlCommand(createTableQuery, dbConnection);
        await command.ExecuteNonQueryAsync();

        await dbConnection.CloseAsync();
    }



    private async Task DeleteDatabaseIfItExists(int accountId, string connectionString)
    {
        string dbName = $"\"AuditLog_{accountId}\"";
        using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();

            if (await DatabaseExists(dbName, connection))
            {
                await TerminateAllConnectionsToDatabase(dbName, connection);
                await DropDatabase(dbName, connection);
            }

            await connection.CloseAsync();
        }
    }

    private async Task TerminateAllConnectionsToDatabase(string dbName, NpgsqlConnection connection)
    {
        var command = new NpgsqlCommand($"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{dbName.Replace("\"", "")}' AND pid <> pg_backend_pid();", connection);
        await command.ExecuteNonQueryAsync();
    }

    private async Task DropDatabase(string dbName, NpgsqlConnection connection)
    {
        var command = new NpgsqlCommand($"DROP DATABASE {dbName}", connection);
        await command.ExecuteNonQueryAsync();
    }



    private async Task<DataTable> ReadTaskgroupCsvIntoDataTable(string csvFilePath)
    {
        var taskGroups = new DataTable();

        // Add columns to DataTable
        taskGroups.Columns.AddRange(new DataColumn[]
        {
        new DataColumn("Account_Id", typeof(int)),
        new DataColumn("Taskgroup_Id", typeof(int)),
        new DataColumn("Guid", typeof(Guid)),
        new DataColumn("Name", typeof(string)),
        new DataColumn("GlobalID", typeof(string))
        });

        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = false,
            Delimiter = ",",
            BadDataFound = null
        };

        using (var reader = new StreamReader(csvFilePath))
        using (var csv = new CsvReader(reader, config))
        {
            while (await csv.ReadAsync())
            {
                taskGroups.Rows.Add(CreateTaskgroupRowFromCsvRecord(csv, taskGroups));
            }
        }

        // Sort the data based on Account_Id
        DataView dv = taskGroups.DefaultView;
        dv.Sort = "Account_Id asc";
        return dv.ToTable();
    }

    private DataRow CreateTaskgroupRowFromCsvRecord(CsvReader csv, DataTable table)
    {
        var accountId = csv.GetField(0);
        var taskgroupId = csv.GetField(1);
        var guidString = csv.GetField(3);
        var name = csv.GetField(2);
        var globalId = csv.GetField(4);

        if (!Guid.TryParse(guidString, out Guid guid))
        {
            Console.WriteLine($"Unable to parse '{guidString}' to a Guid.");
            return null;
        }

        var row = table.NewRow();
        row["Account_Id"] = int.Parse(accountId);
        row["Taskgroup_Id"] = int.Parse(taskgroupId);
        row["Guid"] = guid;
        row["Name"] = name;
        row["GlobalID"] = globalId;

        return row;
    }
    private async Task InsertTaskgroupsIntoDatabase(IGrouping<object, DataRow> group, string connectionString)
    {
        string dbName = $"\"AuditLog_{group.Key}\"";
        using (var connection = new NpgsqlConnection(connectionString + $"Database={dbName};"))
        {
            try
            {
                await connection.OpenAsync();

                using (var writer = connection.BeginBinaryImport("COPY \"TaskGroup\" (\"Taskgroup_Id\", \"Guid\", \"Name\", \"GlobalID\") FROM STDIN (FORMAT BINARY)"))
                {
                    foreach (var row in group)
                    {
                        writer.StartRow();
                        writer.Write(row["Taskgroup_Id"], NpgsqlTypes.NpgsqlDbType.Integer);
                        writer.Write(row["Guid"], NpgsqlTypes.NpgsqlDbType.Uuid);
                        writer.Write(row["Name"], NpgsqlTypes.NpgsqlDbType.Text);
                        writer.Write(row["GlobalID"], NpgsqlTypes.NpgsqlDbType.Text);
                    }

                    await writer.CompleteAsync();
                }
            }
            catch (Npgsql.PostgresException ex)
            {
                if (ex.SqlState == "3D000")
                {
                    Console.WriteLine($"Database {dbName} does not exist. Skipping...");
                    return;
                }

                throw;
            }
            finally
            {
                await connection.CloseAsync();
            }
        }
    }

    // public async Task InsertDataIntoDatabasesAsync(List<AuditLog> auditLogs)
    // {
    //     string connectionString = "Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;TrustServerCertificate=True;";
    //     var groupedAuditLogs = auditLogs.GroupBy(a => a.AccountId);

    //     var config = new CsvConfiguration(CultureInfo.InvariantCulture)
    //     {
    //         HasHeaderRecord = false, // Set to true if your CSV file has a header
    //         Delimiter = ",",
    //         BadDataFound = null
    //     };

    //     foreach (var group in groupedAuditLogs)
    //     {
    //         Console.WriteLine($"Processing account {group.Key}");
    //         if (group.Key.HasValue)
    //         {
    //             string dbName = $"\"AuditLog_{group.Key.Value}\"";
    //             using (var connection = new NpgsqlConnection(connectionString + $"Database={dbName};"))
    //             {
    //                 await connection.OpenAsync();

    //                 using (var writer = connection.BeginBinaryImport("COPY \"AuditLog\" (\"PUser_Id\", \"ImpersonatedUser_Id\", \"Type\", \"Table\", \"Log\", \"Created\") FROM STDIN (FORMAT BINARY)"))
    //                 {
    //                     foreach (var auditLog in group)
    //                     {
    //                         writer.StartRow();
    //                         writer.Write(auditLog.PUser_Id, NpgsqlTypes.NpgsqlDbType.Integer);
    //                         writer.Write(auditLog.ImpersonatedUser_Id, NpgsqlTypes.NpgsqlDbType.Integer);
    //                         writer.Write(auditLog.Type, NpgsqlTypes.NpgsqlDbType.Smallint);
    //                         writer.Write(auditLog.Table, NpgsqlTypes.NpgsqlDbType.Text);
    //                         writer.Write(auditLog.Log, NpgsqlTypes.NpgsqlDbType.Text);
    //                         writer.Write(auditLog.Created, NpgsqlTypes.NpgsqlDbType.Timestamp);
    //                     }

    //                     await writer.CompleteAsync();
    //                 }

    //                 await connection.CloseAsync();
    //             }
    //         }
    //     }
    // }

}