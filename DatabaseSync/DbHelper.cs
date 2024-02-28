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

    public void CreateLogsTable()
    {
        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            // Check if the Logs table already exists
            using (var checkTableCommand = new SqlCommand(
                "IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Logs') " +
                "BEGIN " +
                "CREATE TABLE Logs (Id INT PRIMARY KEY IDENTITY(1,1), Month NVARCHAR(50), LogData NVARCHAR(MAX)) " +
                "END",
                connection))
            {
                checkTableCommand.ExecuteNonQuery();
            }
        }
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

    public static async Task<DataSet> QueryCDCTablesAsync(string connectionString)
    {
        using (var connection = new SqlConnection(connectionString))
        {
            await connection.OpenAsync();

            // Select changes from CDC tables
            string query = @"
        SELECT
            [__$operation],
            [__$start_lsn],
            [__$end_lsn],
            [__$seqval],
            [__$update_mask],
            [Id],
            [Month],
            [LogData],
            [__$command_id]
        FROM
            cdc.dbo_Logs_CT";

            using (var command = new SqlCommand(query, connection))
            {
                using (var reader = await command.ExecuteReaderAsync())
                {
                    var dataSet = new DataSet();
                    do
                    {
                        // Create new DataTable
                        var table = new DataTable();
                        dataSet.Tables.Add(table);

                        // Load the data from the reader into the DataTable
                        table.Load(reader);
                        table.TableName = "dbo.Logs"; // TODO temporary hardcoded, find out why the table name is "Table" and not "dbo.Logs

                        // Set the primary key for the DataTable
                        if (table.Columns.Contains("__$start_lsn") && table.Columns.Contains("__$seqval") && table.Columns.Contains("__$operation"))
                        {
                            DataColumn? startLsnColumn = table.Columns["__$start_lsn"];
                            DataColumn? seqvalColumn = table.Columns["__$seqval"];
                            DataColumn? operationColumn = table.Columns["__$operation"];

                            if (startLsnColumn != null && seqvalColumn != null && operationColumn != null)
                            {
                                table.PrimaryKey = new DataColumn[] { startLsnColumn, seqvalColumn, operationColumn };
                            }
                        }

                        // Convert the operation column to an integer
                        foreach (DataRow row in table.Rows)
                        {
                            int operation = Convert.ToInt32(row["__$operation"]);
                            row["__$operation"] = operation;
                        }
                    }
                    while (!reader.IsClosed);

                    connection.Close();

                    return dataSet;
                }
            }
        }
    }

    public async Task EmptyDatabaseTableDboLogsAsync()
    {
        using (var connection = new SqlConnection(connectionString))
        {
            await connection.OpenAsync();

            using (var command = new SqlCommand())
            {
                command.Connection = connection;

                command.CommandText = "DELETE FROM dbo.Logs";
                await command.ExecuteNonQueryAsync();
            }

            connection.Close();
        }
    }

    public async Task EmptyDatabaseCDCTableDboLogsAsync()
    {
        using (var connection = new SqlConnection(connectionString))
        {
            await connection.OpenAsync();

            using (var command = new SqlCommand())
            {
                command.Connection = connection;
                Console.WriteLine("Deleting from cdc.dbo_Logs_CT");

                // // Get the highest LSN from the cdc.lsn_time_mapping table
                // command.CommandText = "SELECT TOP 1 start_lsn FROM cdc.lsn_time_mapping ORDER BY start_lsn DESC";
                // var lsn = await command.ExecuteScalarAsync();
                // Console.WriteLine($"lsn: {lsn}");

                // // Use the LSN as the @low_water_mark parameter in the call to sp_cdc_cleanup_change_table
                // command.CommandText = $"EXECUTE sys.sp_cdc_cleanup_change_table @capture_instance = 'dbo_Logs', @low_water_mark = @lsn, @threshold = 50000";
                // command.Parameters.AddWithValue("@lsn", lsn);

                command.CommandText = "DELETE FROM cdc.dbo_Logs_CT";
                await command.ExecuteNonQueryAsync();
            }

            connection.Close();
        }
    }

}
