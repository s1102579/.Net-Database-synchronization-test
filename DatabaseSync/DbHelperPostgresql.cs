

using System.Data;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using DatabaseSync.Entities;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
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

        DataTable taskGroups = await ReadTaskgroupCsvIntoDataTableAsync(csvFilePath);

        // Sort and group the data based on Account_Id
        var groupedData = taskGroups.AsEnumerable().GroupBy(row => row["Account_Id"]);

        foreach (var group in groupedData)
        {
            await InsertTaskgroupsIntoDatabaseAsync(group, connectionString);
        }
    }

    public async Task InsertPUserDataIntoDatabasesfromCsvFileAsync(string csvFilePath)
    {
        string connectionString = "Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;TrustServerCertificate=True;";

        DataTable pUsers = await ReadPUserCsvIntoDataTableAsync(csvFilePath);

        // Sort and group the data based on PUser_Id
        var groupedData = pUsers.AsEnumerable().GroupBy(row => row["Account_Id"]);

        foreach (var group in groupedData)
        {
            await InsertPUsersIntoDatabaseAsync(group, connectionString);
        }
    }


    public async Task InsertAuditLogsIntoDatabaseAsync(List<AuditLog> auditLogs)
    {
        // Group the audit logs by AccountId
        var groupedLogs = auditLogs.GroupBy(log => log.AccountId);

        foreach (var group in groupedLogs)  // TODO should start with group.key = 13 
        {
            if (group.Key == null)
            {
                continue; // Skip processing if AccountId is null
            }
            Console.WriteLine($"Processing account {group.Key}");

            // Split the audit logs into different tables
            var pUsers = group.Where(log => log.PUser_Id != null).Select(log => new { PUser_Id = log.PUser_Id }).Distinct();
            // if User is not in the Pusers data from this accountId, it means it is a admin account from pepperflow. These accounts should be added but without name and email
            var newPUsers = await GetNewPUsersAsync(new NpgsqlConnection($"Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;Database=AuditLog_{group.Key};TrustServerCertificate=True;"), pUsers);
            var data = group.Select((log, index) => new { Data_Id = index + 1, Table = log.Table, Data = log.Log });
            var types = group.Select(log => log.Type).Distinct()
                             .Select(typeId => new
                             {
                                 Type_Id = typeId,
                                 Name = typeId switch
                                 {
                                     0 => "ACTION-EXECUTE",
                                     1 => "INSERT",
                                     2 => "UPDATE",
                                     3 => "DELETE",
                                     _ => "UNKNOWN"
                                 }
                             });
            var operationPUsers = group.Where(log => log.PUser_Id != null).Select((log, index) => new { PUser_Id = log.PUser_Id, Operation_Id = index + 1 });
            var operations = group.Where(log => log.PUser_Id != null).Select((log, index) => new { Operation_Id = index + 1, Type_Id = log.Type, Data_Id = index + 1, Created = log.Created }); // maybe remove the Puser is not null stuff later

            if (group.Key == 13)
            {
                Console.WriteLine("All Pepperflow emplyees");
                // log all PUsers
                foreach (var pUser in pUsers)
                {
                    Console.WriteLine(pUser);
                }
            }

            // Insert the data into the different tables
            using (var connection = new NpgsqlConnection($"Host=localhost;Port=5432;Username=postgres;Password=Your_Strong_Password;Database=AuditLog_{group.Key};TrustServerCertificate=True;"))
            {
                await connection.OpenAsync();

                await InsertDataAsync(connection, "\"PUser\"", newPUsers);
                await InsertDataAsync(connection, "\"Data\"", data);
                await InsertDataAsync(connection, "\"Type\"", types);
                await InsertDataAsync(connection, "\"OperationPUser\"", operationPUsers);
                await InsertDataAsync(connection, "\"Operation\"", operations);

                // await AlterDataColumnToJsonbAsync(connection, "\"Data\"");  // TODO probably not the best solution, but remember to alter it back to text when data needs to be added again.

                await connection.CloseAsync();
            }
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
                await CreateDatabaseAndTablesIfNotExistsAsync(auditLog.AccountId.Value, connectionString);
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
            await DeleteDatabaseIfItExistsAsync(accountId, connectionString);
        }
    }

    public async Task AddRowsToAuditLogTableWithCSVFileExceptForOneDayAsync(string path)
    {
        var auditLogs = await ReadAuditLogsFromCSVAsync(path);
        await WriteAuditLogsToDatabase(auditLogs);
    }

    /* ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- */
    /* Private methods */

    private async Task<DataTable> ReadAuditLogsFromCSVAsync(string path)
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

    private async Task CreateDatabaseAndTablesIfNotExistsAsync(int accountId, string connectionString)
    {
        string dbName = $"\"AuditLog_{accountId}\"";
        using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();

            if (!await DatabaseExistsAsync(dbName, connection))
            {
                await CreateDatabase(dbName, connection);
                await CreateTablesInDatabaseAsync(dbName, connectionString);
            }

            await connection.CloseAsync();
        }
    }

    private async Task<bool> DatabaseExistsAsync(string dbName, NpgsqlConnection connection)
    {
        var command = new NpgsqlCommand($"SELECT 1 FROM pg_database WHERE datname = '{dbName.Replace("\"", "")}'", connection);
        return await command.ExecuteScalarAsync() != null;
    }

    private async Task CreateDatabase(string dbName, NpgsqlConnection connection)
    {
        var command = new NpgsqlCommand($"CREATE DATABASE {dbName}", connection);
        await command.ExecuteNonQueryAsync();
    }

    private async Task CreateTablesInDatabaseAsync(string dbName, string connectionString)
    {
        var dbConnection = new NpgsqlConnection(connectionString + $"Database={dbName};");
        await dbConnection.OpenAsync();

        string createTableQuery = @"
            CREATE TABLE ""PUser"" (
                ""PUser_Id"" integer PRIMARY KEY,
                ""FirstName"" varchar(128),
                ""LastName"" varchar(128),
                ""Email"" varchar(128),
                ""Guid"" uuid
            );

            CREATE TABLE ""Operation"" (
                ""Operation_Id"" integer PRIMARY KEY,
                ""Type_Id"" smallint,
                ""Data_Id"" integer,
                ""Created"" timestamp
            );

            CREATE TABLE ""OperationPUser"" (
                ""PUser_Id"" integer,
                ""Operation_Id"" integer,
                PRIMARY KEY (""PUser_Id"", ""Operation_Id"")
            );

            CREATE TABLE ""Data"" (
                ""Data_Id"" integer PRIMARY KEY,
                ""Table"" varchar(128),
                ""Data"" text
            );

            CREATE TABLE ""Type"" (
                ""Type_Id"" smallint PRIMARY KEY,
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

    private async Task DeleteDatabaseIfItExistsAsync(int accountId, string connectionString)
    {
        string dbName = $"\"AuditLog_{accountId}\"";
        using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();

            if (await DatabaseExistsAsync(dbName, connection))
            {
                await TerminateAllConnectionsToDatabaseAsync(dbName, connection);
                await DropDatabaseAsync(dbName, connection);
            }

            await connection.CloseAsync();
        }
    }

    private async Task TerminateAllConnectionsToDatabaseAsync(string dbName, NpgsqlConnection connection)
    {
        var command = new NpgsqlCommand($"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{dbName.Replace("\"", "")}' AND pid <> pg_backend_pid();", connection);
        await command.ExecuteNonQueryAsync();
    }

    private async Task DropDatabaseAsync(string dbName, NpgsqlConnection connection)
    {
        var command = new NpgsqlCommand($"DROP DATABASE {dbName}", connection);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<DataTable> ReadTaskgroupCsvIntoDataTableAsync(string csvFilePath)
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

    private async Task<DataTable> ReadPUserCsvIntoDataTableAsync(string csvFilePath)
    {
        var pUsers = new DataTable();

        // Add columns to DataTable
        pUsers.Columns.AddRange(new DataColumn[]
        {
        new DataColumn("Account_Id", typeof(int)),
        new DataColumn("PUser_Id", typeof(int)),
        new DataColumn("Firstname", typeof(string)),
        new DataColumn("Lastname", typeof(string)),
        new DataColumn("Email", typeof(string)),
        new DataColumn("Guid", typeof(Guid)),
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
                pUsers.Rows.Add(CreatePUserRowFromCsvRecord(csv, pUsers));
            }
        }

        // Sort the data based on Account_Id
        DataView dv = pUsers.DefaultView;
        dv.Sort = "Account_Id asc";
        return dv.ToTable();
    }

    private DataRow CreatePUserRowFromCsvRecord(CsvReader csv, DataTable table)
    {
        var accountId = csv.GetField(1);
        var PUser_Id = csv.GetField(0);
        var guidString = csv.GetField(5);
        var Firstname = csv.GetField(3);
        var Lastname = csv.GetField(4);
        var Email = csv.GetField(2);

        var row = table.NewRow();
        row["Account_Id"] = int.Parse(accountId);
        row["PUser_Id"] = int.Parse(PUser_Id);
        row["Guid"] = Guid.Parse(guidString);
        row["Firstname"] = Firstname;
        row["Lastname"] = Lastname;
        row["Email"] = Email;

        return row;
    }

    private DataRow CreateTaskgroupRowFromCsvRecord(CsvReader csv, DataTable table)
    {
        var accountId = csv.GetField(0);
        var taskgroupId = csv.GetField(1);
        var guidString = csv.GetField(3);
        var name = csv.GetField(2);
        var globalId = csv.GetField(4);

        var row = table.NewRow();
        row["Account_Id"] = int.Parse(accountId);
        row["Taskgroup_Id"] = int.Parse(taskgroupId);
        row["Guid"] = Guid.Parse(guidString);
        row["Name"] = name;
        row["GlobalID"] = globalId;

        return row;
    }
    private async Task InsertTaskgroupsIntoDatabaseAsync(IGrouping<object, DataRow> group, string connectionString)
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

    private async Task InsertPUsersIntoDatabaseAsync(IGrouping<object, DataRow> group, string connectionString)
    {
        string dbName = $"\"AuditLog_{group.Key}\"";
        using (var connection = new NpgsqlConnection(connectionString + $"Database={dbName};"))
        {
            try
            {
                await connection.OpenAsync();

                using (var writer = connection.BeginBinaryImport("COPY \"PUser\" (\"PUser_Id\", \"Email\", \"FirstName\", \"LastName\", \"Guid\") FROM STDIN (FORMAT BINARY)"))
                {
                    foreach (var row in group)
                    {
                        writer.StartRow();
                        writer.Write(row["PUser_Id"], NpgsqlTypes.NpgsqlDbType.Integer);
                        writer.Write(row["Email"], NpgsqlTypes.NpgsqlDbType.Text);
                        writer.Write(row["Firstname"], NpgsqlTypes.NpgsqlDbType.Text);
                        writer.Write(row["Lastname"], NpgsqlTypes.NpgsqlDbType.Text);
                        writer.Write(row["Guid"], NpgsqlTypes.NpgsqlDbType.Uuid);
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

    // private void InsertDataIntoDataTable<T>(NpgsqlConnection connection, IEnumerable<T> data)
    // {
    //     using (var writer = connection.BeginTextImport($"COPY \"Data\" (\"Data_Id\", \"Table\", \"Data\") FROM STDIN"))
    //     {
    //         foreach (var item in data)
    //         {
    //             var props = typeof(T).GetProperties();
    //             var values = new List<string>();
    //             foreach (var prop in props)
    //             {
    //                 var value = prop.GetValue(item);
    //                 if (prop.Name == "Data")
    //                 {
    //                     var jsonValue = value is string ? value.ToString() : JsonConvert.SerializeObject(value);
    //                     values.Add(jsonValue);
    //                 }
    //                 else
    //                 {
    //                     values.Add(value.ToString());
    //                 }
    //             }

    //             writer.WriteLine(string.Join("\t", values));
    //         }

    //         writer.Close();
    //     }
    // }


    private async Task InsertDataAsync<T>(NpgsqlConnection connection, string tableName, IEnumerable<T> data)
    {
        using (var writer = connection.BeginBinaryImport($"COPY {tableName} ({string.Join(", ", typeof(T).GetProperties().Select(p => $"\"{p.Name}\""))}) FROM STDIN (FORMAT BINARY)"))
        {
            foreach (var item in data)
            {
                Console.WriteLine($"Inserting {item} into {tableName}");
                writer.StartRow();

                foreach (var prop in typeof(T).GetProperties())
                {
                    writer.Write(prop.GetValue(item));
                }
            }

            await writer.CompleteAsync();
        }
    }

    private async Task AlterDataColumnToJsonbAsync(NpgsqlConnection connection, string tableName)
    {
        using (var cmd = new NpgsqlCommand($"ALTER TABLE {tableName} ALTER COLUMN \"Data\" TYPE jsonb USING \"Data\"::jsonb", connection))
        {
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private async Task<bool> PUserExistsAsync(NpgsqlConnection connection, dynamic pUser)
    {

        bool exists = false;
        try
        {
            await connection.OpenAsync();
            using (var cmd = new NpgsqlCommand("SELECT COUNT(*) FROM \"PUser\" WHERE \"PUser_Id\" = @PUser_Id", connection))
            {
                cmd.Parameters.AddWithValue("@PUser_Id", pUser.PUser_Id);
                var count = (long)await cmd.ExecuteScalarAsync();
                exists = count > 0;
            }
        }
        finally
        {
            await connection.CloseAsync();
        }
        return exists;
    }

private async Task<List<T>> GetNewPUsersAsync<T>(NpgsqlConnection connection, IEnumerable<T> pUsers) where T : class
{
    var newPUsers = new List<T>();

    foreach (var pUser in pUsers)
    {
        if (!await PUserExistsAsync(connection, pUser))
        {
            newPUsers.Add(pUser);
        }
    }

    return newPUsers;
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