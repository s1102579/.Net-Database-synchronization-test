// count the amount of unique JSON structures in the Log column
DbHelper dbHelper = new DbHelper(SqlServerDbContext.Instance);
HashSet<string> uniqueJsonStructures = await dbHelper.GetDistinctJsonStructuresAsync("assets/AuditLogData.csv");

var trulyUniqueJsonStructures = dbHelper.RemoveSubsets(uniqueJsonStructures);

Console.WriteLine("Amount of truly unique JSON structures: " + trulyUniqueJsonStructures.Count);

// foreach (var structure in trulyUniqueJsonStructures)
// {
//     Console.WriteLine();
//     Console.WriteLine(structure);
//     Console.WriteLine();
// }

// find unique keys in Json file
var uniqueKeys = await dbHelper.FindUniqueKeysAsync("assets/AuditLogData.csv");
Console.WriteLine("Amount of unique keys: " + uniqueKeys.Count);
// int tempCount = 0;
// foreach (var key in uniqueKeys)
// {
//     if (key.Contains('.'))
//     {
//         continue;
//     }
//     Console.WriteLine(key);
//     tempCount++;
// }



