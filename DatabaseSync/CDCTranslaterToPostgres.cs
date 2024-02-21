using System.Data;

public class CDCTranslaterToPostgres
{
    public static string TranslateToPostgreSQLQuery(DataRow change, string tableName)
    {
        // string tableName = change["Month"].ToString(); // This returns the column value now

        Console.WriteLine($"Translating change for table {tableName}");
        Console.WriteLine(string.Join(", ", change.ItemArray));

        // convert $__operation to integer 
        // TODO move this code elsewhere more appropriate
        int operation = Convert.ToInt32(change["__$operation"]);
        // Replace the $__operation value in the row with the integer
        change["__$operation"] = operation;
        Console.WriteLine($"Operation: {operation}");
        switch (operation)
        {
            case 1: // Delete
                return TranslateDeleteToPostgreSQL(tableName, change);
            case 2: // Insert
                return TranslateInsertToPostgreSQL(tableName, change);
            case 3: // Update
                return TranslateUpdateToPostgreSQL(tableName, change);
            case 4: // Update
                return TranslateUpdateToPostgreSQL(tableName, change);
            default:
                throw new NotSupportedException($"Unsupported $__operation value: {operation}");
        }
    }

    public static string TranslateInsertToPostgreSQL(string tableName, DataRow change)
    {
        Console.WriteLine($"Translating insert to PostgreSQL for table {tableName}");

        // Filter out columns that start with __$
        var columnsToUpdate = change.Table.Columns.OfType<DataColumn>()
            .Where(c => !c.ColumnName.StartsWith("__$"));

        var columns = string.Join(", ", columnsToUpdate.Select(c => $"\"{c.ColumnName}\""));
        var values = string.Join(", ", columnsToUpdate.Select(c => $"\'{change[c]}\'"));

        Console.WriteLine($"Columns: {columns}");
        Console.WriteLine($"Values: {values}");

        return $"INSERT INTO public.\"{tableName}\" ({columns}) VALUES ({values}) ON CONFLICT (\"Id\") DO NOTHING;";
    }

    public static string TranslateUpdateToPostgreSQL(string tableName, DataRow change)
    {
        Console.WriteLine($"Translating update to PostgreSQL for table {tableName}");

        // Filter out columns that start with __$
        var columnsToUpdate = change.Table.Columns.OfType<DataColumn>()
            .Where(c => !c.ColumnName.StartsWith("__$"));

        var setClause = string.Join(", ", columnsToUpdate
            .Select(c =>
            {
                var value = change[c, DataRowVersion.Current].ToString();
                // Check if the column is LogData and the value is an empty string
                if (c.ColumnName == "LogData" && string.IsNullOrEmpty(value))
                {
                    // Set the value to a valid JSON value
                    value = "null";
                }
                return $"\"{c.ColumnName}\" = \'{value}\'";
            }));

        // Use Id as the primary key
        var whereClause = $"\"Id\" = \'{change["Id", DataRowVersion.Original]}\'";

        Console.WriteLine($"SET clause: {setClause}");
        Console.WriteLine($"WHERE clause: {whereClause}");

        return $"UPDATE public.\"{tableName}\" SET {setClause} WHERE {whereClause};";
    }

    public static string TranslateDeleteToPostgreSQL(string tableName, DataRow change)
    {
        Console.WriteLine($"Translating delete to PostgreSQL for table {tableName}");

        // Use Id as the primary key
        var whereClause = $"\"Id\" = \'{change["Id", DataRowVersion.Original]}\'";

        Console.WriteLine($"WHERE clause: {whereClause}");

        return $"DELETE FROM public.\"{tableName}\" WHERE {whereClause};";
    }

}

