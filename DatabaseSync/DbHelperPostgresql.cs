

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

    public async Task<List<AuditLog>> GetDataFromAuditLogsTableAsync()
    {
        return await _context.AuditLogs.ToListAsync();
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

    public async void CheckIfDatabaseExists()
    {
        using (var connection = new NpgsqlConnection(_connectionString))
        {
            await connection.OpenAsync();

            Console.WriteLine("Checking if the database exists...");

            // Check if the database exists
            using (var checkDatabaseCommand = new NpgsqlCommand(
                "SELECT 1 FROM pg_database WHERE datname = 'postgres_sync_database'",
                connection))
            {
                var databaseExists = await checkDatabaseCommand.ExecuteScalarAsync();
                if (databaseExists == null)
                {
                    Console.WriteLine("Database does not exist. Creating...");
                    using (var createDatabaseCommand = new NpgsqlCommand(
                        "CREATE DATABASE postgres_sync_database",
                        connection))
                    {
                        await createDatabaseCommand.ExecuteNonQueryAsync();
                    }
                }
            }
            connection.Close();
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

    public async Task AddRowsToAuditLogTableWithCSVFileAsync(string path)
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
                var accountId = csv.GetField(0);
                var pUserId = csv.GetField(1);
                var impersonatedUserId = csv.GetField(2);
                var type = csv.GetField(3);
                var table = csv.GetField(4);
                var log = csv.GetField(5);
                var created = csv.GetField(6);

                // Console.WriteLine(accountId + " " + pUserId + " " + impersonatedUserId + " " + type + " " + table + " " + log + " " + created);


                var row = auditLogs.NewRow();
                row["AccountId"] = accountId == "NULL" ? DBNull.Value : int.Parse(accountId);
                row["PUser_Id"] = pUserId == "NULL" ? DBNull.Value : int.Parse(pUserId);
                row["ImpersonatedUser_Id"] = impersonatedUserId == "NULL" ? DBNull.Value : int.Parse(impersonatedUserId);
                row["Type"] = byte.Parse(type);
                row["Table"] = table;
                row["Log"] = log;
                row["Created"] = DateTime.Parse(created);

                auditLogs.Rows.Add(row);
            }
        }

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

    public async Task AddRowsToAuditLogTableWithCSVFileExceptForOneDayAsync(string path)
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
                var accountId = csv.GetField(0);
                var pUserId = csv.GetField(1);
                var impersonatedUserId = csv.GetField(2);
                var type = csv.GetField(3);
                var table = csv.GetField(4);
                var log = csv.GetField(5);
                var created = csv.GetField(6);

                Console.WriteLine(accountId + " " + pUserId + " " + impersonatedUserId + " " + type + " " + table + " " + log + " " + created);

                if (!created.Contains("2023-01-31"))
                {
                    var row = auditLogs.NewRow();
                    row["AccountId"] = accountId == "NULL" ? DBNull.Value : int.Parse(accountId);
                    row["PUser_Id"] = pUserId == "NULL" ? DBNull.Value : int.Parse(pUserId);
                    row["ImpersonatedUser_Id"] = impersonatedUserId == "NULL" ? DBNull.Value : int.Parse(impersonatedUserId);
                    row["Type"] = byte.Parse(type);
                    row["Table"] = table;
                    row["Log"] = log;
                    row["Created"] = DateTime.Parse(created);

                    auditLogs.Rows.Add(row);
                }
            }
        }

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

    // public async Task AddRowsToAuditLogTableWithCSVFileExceptForOneDayAsync(string path) // for testing purposes only. Theoretical starting point for postgresql database where it runs one day behine de source database
    // {
    //     var auditLogs = new List<AuditLog>();

    //     var config = new CsvConfiguration(CultureInfo.InvariantCulture)
    //     {
    //         HasHeaderRecord = false, // Set to true if your CSV file has a header
    //         Delimiter = ",",
    //         BadDataFound = null
    //     };

    //     using (var reader = new StreamReader(path))
    //     using (var csv = new CsvReader(reader, config))
    //     {
    //         while (await csv.ReadAsync())
    //         {
    //             var accountId = csv.GetField(0);
    //             var pUserId = csv.GetField(1);
    //             var impersonatedUserId = csv.GetField(2);
    //             var type = csv.GetField(3);
    //             var table = csv.GetField(4);
    //             var log = csv.GetField(5);
    //             var created = csv.GetField(6);

    //             Console.WriteLine(accountId + " " + pUserId + " " + impersonatedUserId + " " + type + " " + table + " " + log + " " + created);
    //             if (!created.Contains("2023-01-31"))
    //             {
    //                 var auditLog = new AuditLog
    //                 {
    //                     AccountId = accountId == "NULL" ? (int?)null : int.Parse(accountId),
    //                     PUser_Id = pUserId == "NULL" ? (int?)null : int.Parse(pUserId),
    //                     ImpersonatedUser_Id = impersonatedUserId == "NULL" ? (int?)null : int.Parse(impersonatedUserId),
    //                     Type = byte.Parse(type),
    //                     Table = table,
    //                     Log = log,
    //                     Created = DateTime.Parse(created)
    //                 };

    //                 auditLogs.Add(auditLog);
    //             }
    //         }
    //     }
    //     await _context.BulkInsertAsync(auditLogs); // zou veel tijd moeten schelen met normale insert zoals de _context.SaveChangesAsync()
    // }
}