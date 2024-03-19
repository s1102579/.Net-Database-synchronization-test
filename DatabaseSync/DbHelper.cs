using System.Data;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using DatabaseSync.Entities;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

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

        Console.WriteLine("Rows added to DataTable: " + auditLogs.Rows.Count);

        using (var sqlBulk = new SqlBulkCopy("Server=localhost,1434;Database=MSSQL_LOG_TEST;User Id=sa;Password=Your_Strong_Password;TrustServerCertificate=True;")) // temp hardcode connection string
        {
            sqlBulk.DestinationTableName = "AuditLog_20230101";
            sqlBulk.BulkCopyTimeout = 600;
            await sqlBulk.WriteToServerAsync(auditLogs);
        }
    }

    public async Task<HashSet<string>> GetDistinctJsonStructuresAsync(string path)
    {
        var jsonStructures = new HashSet<string>();

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
                var log = csv.GetField(5);

                try
                {
                    var json = JObject.Parse(log);
                    var normalizedJson = NormalizeJson(json);
                    jsonStructures.Add(normalizedJson.ToString(Newtonsoft.Json.Formatting.None));
                }
                catch (JsonReaderException)
                {
                    // Handle or log the error for invalid JSON strings
                    // System.Console.WriteLine("Invalid JSON string: " + log);
                }
            }
        }

        return jsonStructures;
    }

    private JObject NormalizeJson(JToken token)
    {
        var obj = new JObject();

        foreach (var property in token.Children<JProperty>())
        {
            if (property.Value is JValue)
            {
                obj.Add(property.Name, null);
            }
            else if (property.Value is JArray array)
            {
                obj.Add(property.Name, new JArray(array.Children().Select(item => NormalizeJson(item))));
            }
            else if (property.Value is JObject)
            {
                obj.Add(property.Name, NormalizeJson(property.Value));
            }
        }

        return obj;
    }

    // public HashSet<string> RemoveSubsets(HashSet<string> jsonStructures)
    // {
    //     // Converteer de HashSet naar een lijst van genormaliseerde sleutelsets.
    //     var jsonKeySets = jsonStructures.Select(str => 
    //         new HashSet<string>(GetKeys(JObject.Parse(str))))
    //         .ToList();

    //     // Bereid een HashSet voor om de unieke structuren op te slaan.
    //     var uniqueStructures = new HashSet<HashSet<string>>(HashSet<string>.CreateSetComparer());

    //     foreach (var keySet in jsonKeySets)
    //     {
    //         bool isSubset = uniqueStructures.Any(existingSet => keySet.IsSubsetOf(existingSet));
    //         if (!isSubset)
    //         {
    //             uniqueStructures.Add(keySet);
    //         }
    //     }

    //     // Converteer de unieke sleutelsets terug naar JSON-strings.
    //     var uniqueJsonStructures = new HashSet<string>();
    //     foreach (var keySet in uniqueStructures)
    //     {
    //         var jsonObj = new JObject();
    //         foreach (var key in keySet)
    //         {
    //             // Voeg sleutels toe aan het JSON-object.
    //             AddKeys(jsonObj, key);
    //         }
    //         uniqueJsonStructures.Add(jsonObj.ToString(Newtonsoft.Json.Formatting.None));
    //     }

    //     return uniqueJsonStructures;
    // }

    public HashSet<string> RemoveSubsets(HashSet<string> jsonStructures)
    {
        // Convert the HashSet into a list of JObjects to preserve the structure.
        var jsonObjects = jsonStructures.Select(str => JObject.Parse(str)).ToList();

        // Prepare a list to store the unique structures.
        var uniqueStructures = new List<JObject>();

        foreach (var json in jsonObjects)
        {
            // Check if the current json is a subset of any already in uniqueStructures.
            if (!uniqueStructures.Any(existing => IsSubset(existing, json)))
            {
                // Remove any existing jsons that are subsets of the current one.
                uniqueStructures.RemoveAll(existing => IsSubset(json, existing));
                uniqueStructures.Add(json);
            }
        }

        // Convert the unique JObjects back into strings.
        return uniqueStructures.Select(obj => obj.ToString(Newtonsoft.Json.Formatting.None)).ToHashSet();
    }

    private bool IsSubset(JObject superSet, JObject subSet)
    {
        var superSetProperties = superSet.Properties().Select(p => p.Name).ToHashSet();
        var subSetProperties = subSet.Properties().Select(p => p.Name).ToHashSet();

        // Check if all properties in subSet are found in superSet.
        return subSetProperties.IsSubsetOf(superSetProperties);
    }

    public async Task<HashSet<string>> FindUniqueKeysAsync(string path)
{
    var uniqueKeys = new HashSet<string>();
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
            var record = csv.GetField(5);
            try
            {
                var json = JObject.Parse(record);
                foreach (var key in GetKeys(json))
                {
                    uniqueKeys.Add(key);
                }
            }
            catch (JsonReaderException)
            {
                // Console.WriteLine($"Ongeldige JSON-structuur gevonden: {record}");
            }
        }
    }

    return uniqueKeys;
}


    private void AddKeys(JObject jsonObj, string key)
    {
        var parts = key.Split('.');
        JObject currentObj = jsonObj;
        foreach (var part in parts)
        {
            if (!currentObj.ContainsKey(part))
            {
                currentObj.Add(part, null);
            }
            currentObj = currentObj[part] as JObject ?? new JObject();
        }
    }

    // public async Task<List<JObject>> FindUniqueJsonStructuresAsync(string path)
    // {
    //     var allStructures = new List<JObject>();
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
    //             var record = csv.GetField(5);
    //             try
    //             {
    //                 var json = JObject.Parse(record);
    //                 allStructures.Add(json);
    //             }
    //             catch (JsonReaderException)
    //             {
    //                 // Console.WriteLine($"Ongeldige JSON-structuur gevonden: {record}");
    //             }
    //         }
    //     }

    //     // Vind unieke structuren.
    //     var uniqueStructures = allStructures
    //         .Select(structure => new
    //         {
    //             Structure = structure,
    //             Keys = GetKeys(structure).OrderBy(key => key).ToList()
    //         })
    //         .ToList();

    //     var trulyUniqueStructures = new List<JObject>();

    //     foreach (var structure in uniqueStructures)
    //     {
    //         bool isSubset = uniqueStructures.Any(other =>
    //             other.Keys.Count > structure.Keys.Count &&
    //             !other.Keys.Except(structure.Keys).Any());

    //         if (!isSubset)
    //         {
    //             trulyUniqueStructures.Add(structure.Structure);
    //         }
    //     }

    //     return trulyUniqueStructures;
    // }

    private IEnumerable<string> GetKeys(JToken token)
    {
        if (token is JObject obj)
        {
            foreach (var property in obj.Properties())
            {
                yield return property.Name;
                foreach (var key in GetKeys(property.Value))
                {
                    yield return $"{property.Name}.{key}";
                }
            }
        }
        else if (token is JArray array)
        {
            for (int i = 0; i < array.Count; i++)
            {
                foreach (var key in GetKeys(array[i]))
                {
                    yield return $"[{i}].{key}";
                }
            }
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
