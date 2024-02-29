using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using DatabaseSync.Entities;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;

public class DbHelper
{
    private readonly SqlServerDbContext _context;

    public DbHelper(SqlServerDbContext context)
    {
        _context = context;
    }

    public async Task InsertListOfLogDataAsync(List<Log> logs)
    {
        _context.Logs.AddRange(logs);
        await _context.SaveChangesAsync();
    }

    public async Task InsertLogDataAsync(string month, string logData)
    {
        var log = new Log
        {
            Month = month,
            LogData = logData
        };

        await _context.Logs.AddAsync(log);
        await _context.SaveChangesAsync();
    }

    public async Task UpdateLogDataAsync(string month, string logData, int Id)
    {
        var log = await _context.Logs.FindAsync(Id);
        if (log != null)
        {
            log.Month = month;
            log.LogData = logData;
            await _context.SaveChangesAsync();
        }
        else
        {
            throw new Exception("Log not found");
        }
    }

    public async Task DeleteLogDataAsync(int Id)
    {
        var log = await _context.Logs.FindAsync(Id);
        if (log != null)
        {
            _context.Logs.Remove(log);
            await _context.SaveChangesAsync();
        }
        else
        {
            throw new Exception("Log not found");
        }
    }

    public async Task<List<Log>> GetDataFromLogsTableAsync()
    {
        return await _context.Logs.ToListAsync();
    }


    // public async Task EmptyDatabaseTableDboLogsAsync()
    // {
    //     _context.Logs.RemoveRange(_context.Logs);
    //     await _context.SaveChangesAsync();
    // }

    public async Task EmptyDatabaseTableDboAuditLogsAsync()
    {
        await _context.Database.ExecuteSqlRawAsync("DELETE FROM AuditLog_20230101");
    }

    public async Task<List<AuditLog>> GetDataFromAuditLogsTableAsync()
    {
        return await _context.AuditLogs.ToListAsync();
    }

    public async Task InsertListOfAuditLogDataAsync(List<AuditLog> logs)
    {
        _context.AuditLogs.AddRange(logs);
        await _context.SaveChangesAsync();
    }

    public async Task InsertAuditLogDataAsync(AuditLog log)
    {
        await _context.AuditLogs.AddAsync(log);
        await _context.SaveChangesAsync();
    }

public async Task AddRowsToAuditLogTableWithCSVFileAsync(string path)
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
    await _context.BulkInsertAsync(auditLogs); // zou veel tijd moeten schelen met normale insert zoals de _context.SaveChangesAsync()
}
}
