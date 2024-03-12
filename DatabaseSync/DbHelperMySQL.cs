using System.Data;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using DatabaseSync.Entities;
using Microsoft.EntityFrameworkCore;
using MySqlConnector;
using Npgsql;
using Npgsql.Replication;

public class DbHelperMySQL
{
    private readonly string _connectionString;
    private readonly MySQLDbContext? _context;

    public DbHelperMySQL(MySQLDbContext? context)
    {
        _context = context;
    }


    public async Task EmptyDatabaseTableDboLogsAsync()
    {
        _context.Logs.RemoveRange(_context.Logs);
        await _context.SaveChangesAsync();
    }

    public async Task EmptyDatabaseTableAuditLogsAsync()
    {
        await _context.Database.ExecuteSqlRawAsync("DELETE FROM `AuditLog_20230101`");
    }
    public async Task AddRowsToAuditLogTableWithCSVFileAsync(string path)
    {
        using (var conn = new MySqlConnection("Server=localhost;Database=My_SQL_SPEED_TEST;User Id=root;Password=Your_Strong_Password;AllowLoadLocalInfile=true;"))
        {
            await conn.OpenAsync();

            var bulk = new MySqlBulkLoader(conn)
            {
                TableName = "AuditLog_20230101",
                FieldTerminator = ",",
                LineTerminator = "\n",
                FileName = path,
                NumberOfLinesToSkip = 0, // No header line in the CSV file
            };

            await bulk.LoadAsync();
            conn.Close();
        }
    }
}