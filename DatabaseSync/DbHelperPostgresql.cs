

using System.Data;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using DatabaseSync.Entities;
using EFCore.BulkExtensions;
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

    public async Task InsertListOfAuditLogDataAsync(List<AuditLog> auditLogs)
    {
        await _context.BulkInsertAsync(auditLogs);
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

    public async Task AddRowsToAuditLogTableWithCSVFileExceptForOneDayAsync(string path) // for testing purposes only. Theoretical starting point for postgresql database where it runs one day behine de source database
    {
        var auditLogs = new List<AuditLog>();

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
                    var auditLog = new AuditLog
                    {
                        AccountId = accountId == "NULL" ? (int?)null : int.Parse(accountId),
                        PUser_Id = pUserId == "NULL" ? (int?)null : int.Parse(pUserId),
                        ImpersonatedUser_Id = impersonatedUserId == "NULL" ? (int?)null : int.Parse(impersonatedUserId),
                        Type = byte.Parse(type),
                        Table = table,
                        Log = log,
                        Created = DateTime.Parse(created)
                    };

                    auditLogs.Add(auditLog);
                }
            }
        }
        await _context.BulkInsertAsync(auditLogs); // zou veel tijd moeten schelen met normale insert zoals de _context.SaveChangesAsync()
    }
}