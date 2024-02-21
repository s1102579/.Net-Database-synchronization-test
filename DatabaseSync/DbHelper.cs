using System.Data;
using System.Data.SqlClient;

public class DbHelper
{
    private readonly string connectionString;

    public DbHelper(string connectionString)
    {
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

    public void InsertLogData(string month, string logData)
    {
        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            // Insert a log record into the table
            using (var command = new SqlCommand(
                "INSERT INTO Logs (Month, LogData) VALUES (@Month, @LogData)",
                connection))
            {
                command.Parameters.AddWithValue("@Month", month);
                command.Parameters.AddWithValue("@LogData", logData);

                command.ExecuteNonQuery();
            }
        }
    }

    public static DataSet QueryCDCTables(string connectionString)
    {
        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

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
                var adapter = new SqlDataAdapter(command);
                var dataSet = new DataSet();

                // Fill the DataSet with changes from CDC tables
                adapter.Fill(dataSet);

                return dataSet;
            }
        }
    }
}
