using System.Data;
using System.Data.SqlClient;
using DatabaseSync.Entities;
using Microsoft.EntityFrameworkCore;

public class DbHelper
{
    private readonly SqlServerDbContext _context;
    private readonly string connectionString = "Server=localhost,1433;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;";

    public DbHelper(SqlServerDbContext context, string connectionString)
    {
        _context = context;
        this.connectionString = connectionString;
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


    public async Task EmptyDatabaseTableDboLogsAsync()
    {
        _context.Logs.RemoveRange(_context.Logs);
        await _context.SaveChangesAsync();
    }

}
