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

                // Set the primary key for each DataTable in the DataSet
                foreach (DataTable table in dataSet.Tables)
                {
                    table.TableName = "dbo.Logs"; // TODO temporary hardcoded, find out why the table name is "Table" and not "dbo.Logs
                    if (table.Columns.Contains("__$start_lsn") && table.Columns.Contains("__$seqval") && table.Columns.Contains("__$operation"))
                    {
                        table.PrimaryKey = new DataColumn[] { table.Columns["__$start_lsn"], table.Columns["__$seqval"], table.Columns["__$operation"] };
                    }

                    // Convert the operation column to an integer
                    foreach (DataRow row in table.Rows)
                    {
                        int operation = Convert.ToInt32(row["__$operation"]);
                        row["__$operation"] = operation;
                    }
                }

                return dataSet;
            }
        }
    }
    public void emptyDatabaseTableDboLogs()
    {
        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            using (var command = new SqlCommand())
            {
                command.Connection = connection;
                var databaseName = "MSSQL_LOG_TEST";
                
                command.CommandText = "DELETE FROM dbo.Logs";
                command.ExecuteNonQuery();
            }
        }
    }

    public void emptyDatabaseCDCTableDboLogs() 
    {
        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            using (var command = new SqlCommand())
            {
                command.Connection = connection;
                var databaseName = "MSSQL_LOG_TEST";
                
                command.CommandText = "DELETE FROM cdc.dbo_Logs_CT";
                command.ExecuteNonQuery();
            }
        }
    }


    // public void deleteDatabaseAndCreateNewOne()
    // {
    //     using (var connection = new SqlConnection(connectionString))
    //     {
    //         connection.Open();

    //         using (var command = new SqlCommand())
    //         {
    //             command.Connection = connection;

    //             // Replace 'YourDatabase' with your actual database name
    //             var databaseName = "MSSQL_LOG_TEST";

    //             // // Switch to the master database
    //             // command.CommandText = "USE master;";
    //             // command.ExecuteNonQuery();

    //             // // Set the database to multi-user mode
    //             // command.CommandText = $"ALTER DATABASE {databaseName} SET MULTI_USER;";
    //             // command.ExecuteNonQuery();

    //             // // Create a new database
    //             // command.CommandText = $"CREATE DATABASE {databaseName};";
    //             // command.ExecuteNonQuery();

    //             // Use the new database
    //             command.CommandText = $"USE {databaseName};";
    //             command.ExecuteNonQuery();

    //             // Enable CDC
    //             command.CommandText = "EXEC sys.sp_cdc_enable_db;";
    //             command.ExecuteNonQuery();

    //             CreateLogsTable();

    //             // Enable CDC on a table
    //             // Replace 'YourTable' with your actual table name
    //             var tableName = "Logs";
    //             command.CommandText = $"EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'{tableName}', @role_name = NULL, @supports_net_changes = 1;";
    //             command.ExecuteNonQuery();
    //         }
    //     }
    // }

}
