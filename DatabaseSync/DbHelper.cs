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
            connection.Close();
        }
    }

    public void UpdateLogData(string month, string logData, string Id)
    {
        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            // Update a log record in the table
            using (var command = new SqlCommand(
                "UPDATE Logs SET LogData = @LogData WHERE Month = @Month AND Id = @Id",
                connection))
            {
                command.Parameters.AddWithValue("@Month", month);
                command.Parameters.AddWithValue("@LogData", logData);
                command.Parameters.AddWithValue("@Id", Id);

                command.ExecuteNonQuery();
            }

            connection.Close();
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

                // Set the primary key for each DataTable in the DataSet
                foreach (DataTable table in dataSet.Tables)
                {
                    table.TableName = "dbo.Logs"; // TODO temporary hardcoded, find out why the table name is "Table" and not "dbo.Logs
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
                connection.Close();

                return dataSet;
            }
        }
    }
    public void EmptyDatabaseTableDboLogs()
    {
        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            using (var command = new SqlCommand())
            {
                command.Connection = connection;
                
                command.CommandText = "DELETE FROM dbo.Logs";
                command.ExecuteNonQuery();
            }

            connection.Close();
        }
    }

    public void EmptyDatabaseCDCTableDboLogs() 
    {
        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            using (var command = new SqlCommand())
            {
                command.Connection = connection;
                
                command.CommandText = "DELETE FROM cdc.dbo_Logs_CT";
                command.ExecuteNonQuery();
            }

            connection.Close();
        }
    }

}
