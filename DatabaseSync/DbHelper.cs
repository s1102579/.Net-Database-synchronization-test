using System.Data;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using DatabaseSync.Entities;
using Microsoft.Data.SqlClient;
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

    public async Task<List<AuditLog>> GetOneDayOfDataFromAuditLogsTableAsync(string day) // for testing only. Normally this would be automatically get the previous day's data
    {
        return await _context.AuditLogs.FromSqlRaw($@"SELECT *
            FROM AuditLog_20230101
            WHERE CONVERT(VARCHAR, Created, 23) LIKE '{day}%'").ToListAsync();
    }

    public async Task AddRowsToAuditLogTableWithCSVFileAsync(string path) // runtime is: 27s with SQLBulkCopy
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
            HasHeaderRecord = false,
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

                var row = auditLogs.NewRow();
                row["AccountId"] = accountId == "NULL" ? (object)DBNull.Value : int.Parse(accountId);
                row["PUser_Id"] = pUserId == "NULL" ? (object)DBNull.Value : int.Parse(pUserId);
                row["ImpersonatedUser_Id"] = impersonatedUserId == "NULL" ? (object)DBNull.Value : int.Parse(impersonatedUserId);
                row["Type"] = byte.Parse(type);
                row["Table"] = table;
                row["Log"] = log;
                row["Created"] = DateTime.Parse(created);

                auditLogs.Rows.Add(row);
            }
        }

        using (var sqlBulk = new SqlBulkCopy("Server=localhost,1434;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;TrustServerCertificate=True;")) // temp hardcode connection string
        {
            sqlBulk.BulkCopyTimeout = 3600;
            sqlBulk.DestinationTableName = "AuditLog_20230101";
            await sqlBulk.WriteToServerAsync(auditLogs);
        }
    }

    // public async Task AddRowsToAuditLogTableWithCSVFileAsync(string path) // runtime is: 39 minutes and 32 seconds with SQLRawAsync
    // {
    //     var auditLogs = new List<AuditLog>();

    //     var config = new CsvConfiguration(CultureInfo.InvariantCulture)
    //     {
    //         HasHeaderRecord = false,
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

    //             var auditLog = new AuditLog
    //             {
    //                 AccountId = accountId == "NULL" ? (int?)null : int.Parse(accountId),
    //                 PUser_Id = pUserId == "NULL" ? (int?)null : int.Parse(pUserId),
    //                 ImpersonatedUser_Id = impersonatedUserId == "NULL" ? (int?)null : int.Parse(impersonatedUserId),
    //                 Type = byte.Parse(type),
    //                 Table = table,
    //                 Log = log,
    //                 Created = DateTime.Parse(created)
    //             };

    //             auditLogs.Add(auditLog);
    //         }
    //     }

    //     foreach (var auditLog in auditLogs)
    //     {
    //         var sql = "INSERT INTO AuditLog_20230101 (AccountId, PUser_Id, ImpersonatedUser_Id, [Type], [Table], [Log], Created) VALUES (@AccountId, @PUser_Id, @ImpersonatedUser_Id, @Type, @Table, @Log, @Created)";
    //         await _context.Database.ExecuteSqlRawAsync(sql, new[] {
    //         new SqlParameter("@AccountId", auditLog.AccountId ?? (object)DBNull.Value),
    //         new SqlParameter("@PUser_Id", auditLog.PUser_Id ?? (object)DBNull.Value),
    //         new SqlParameter("@ImpersonatedUser_Id", auditLog.ImpersonatedUser_Id ?? (object)DBNull.Value),
    //         new SqlParameter("@Type", auditLog.Type),
    //         new SqlParameter("@Table", auditLog.Table),
    //         new SqlParameter("@Log", auditLog.Log),
    //         new SqlParameter("@Created", auditLog.Created)
    //     });
    //     }
    // }


    // public async Task AddRowsToAuditLogTableWithCSVFileAsync(string path) // runtime is: 45 seconds with BulkInsert.
    // {
    //     var auditLogs = new List<AuditLog>();

    //     var config = new CsvConfiguration(CultureInfo.InvariantCulture)
    //     {
    //         HasHeaderRecord = false,
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

    //             var auditLog = new AuditLog
    //             {
    //                 AccountId = accountId == "NULL" ? (int?)null : int.Parse(accountId),
    //                 PUser_Id = pUserId == "NULL" ? (int?)null : int.Parse(pUserId),
    //                 ImpersonatedUser_Id = impersonatedUserId == "NULL" ? (int?)null : int.Parse(impersonatedUserId),
    //                 Type = byte.Parse(type),
    //                 Table = table,
    //                 Log = log,
    //                 Created = DateTime.Parse(created)
    //             };

    //             auditLogs.Add(auditLog);
    //         }
    //     }

    //     await _context.BulkInsertAsync(auditLogs);
    // }


}
