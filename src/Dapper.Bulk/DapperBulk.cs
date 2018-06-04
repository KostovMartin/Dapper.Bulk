using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Dapper.Bulk
{
    /// <summary>
    /// Bulk inserts for Dapper
    /// </summary>
    public static class DapperBulk
    {
        /// <summary>
        /// Inserts entities into table <typeparamref name="T"/>s (by default).
        /// </summary>
        /// <typeparam name="T">The type being inserted.</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="data">Entities to insert</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="batchSize">Number of bulk items inserted together, 0 (the default) if all</param>
        /// <param name="bulkCopyTimeout">Number of seconds before bulk command execution timeout, 30 (the default)</param>
        public static void BulkInsert<T>(this SqlConnection connection, IEnumerable<T> data, SqlTransaction transaction = null, int batchSize = 0, int bulkCopyTimeout = 30)
        {
            var type = typeof(T);
            var tableName = TableMapper.GetTableName(type);
            var allProperties = PropertiesCache.TypePropertiesCache(type);
            var keyProperties = PropertiesCache.KeyPropertiesCache(type);
            var computedProperties = PropertiesCache.ComputedPropertiesCache(type);

            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();
            var allPropertiesExceptKeyAndComputedString = GetColumnsStringSqlServer(allPropertiesExceptKeyAndComputed);
            var tempToBeInserted = $"#{tableName}_TempInsert".Replace(".", string.Empty);

            connection.Execute($@"SELECT TOP 0 {allPropertiesExceptKeyAndComputedString} INTO {tempToBeInserted} FROM {tableName} target WITH(NOLOCK);", null, transaction);

            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout;
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = tempToBeInserted;
                bulkCopy.WriteToServer(ToDataTable(data, tableName, allPropertiesExceptKeyAndComputed).CreateDataReader());
            }

            connection.Execute($@"
                INSERT INTO {tableName}({allPropertiesExceptKeyAndComputedString}) 
                SELECT {allPropertiesExceptKeyAndComputedString} FROM {tempToBeInserted}

                DROP TABLE {tempToBeInserted};", null, transaction);
        }

        /// <summary>
        /// Inserts entities into table <typeparamref name="T"/>s (by default) returns inserted entities.
        /// </summary>
        /// <typeparam name="T">The element type of the array</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="data">Entities to insert</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="batchSize">Number of bulk items inserted together, 0 (the default) if all</param>
        /// <param name="bulkCopyTimeout">Number of seconds before bulk command execution timeout, 30 (the default)</param>
        /// <returns>Inserted entities</returns>
        public static IEnumerable<T> BulkInsertAndSelect<T>(this SqlConnection connection, IEnumerable<T> data, SqlTransaction transaction = null, int batchSize = 0, int bulkCopyTimeout = 30)
        {
            var type = typeof(T);
            var tableName = TableMapper.GetTableName(type);
            var allProperties = PropertiesCache.TypePropertiesCache(type);
            var keyProperties = PropertiesCache.KeyPropertiesCache(type);
            var computedProperties = PropertiesCache.ComputedPropertiesCache(type);

            if (keyProperties.Count == 0)
            {
                var dataList = data.ToList();
                connection.BulkInsert(dataList, transaction, batchSize, bulkCopyTimeout);
                return dataList;
            }

            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            var keyPropertiesString = GetColumnsStringSqlServer(keyProperties);
            var keyPropertiesInsertedString = GetColumnsStringSqlServer(keyProperties, "inserted.");
            var allPropertiesExceptKeyAndComputedString = GetColumnsStringSqlServer(allPropertiesExceptKeyAndComputed);
            var allPropertiesString = GetColumnsStringSqlServer(allProperties, "target.");

            var tempToBeInserted = $"#{tableName}_TempInsert".Replace(".", string.Empty);
            var tempInsertedWithIdentity = $"@{tableName}_TempInserted".Replace(".", string.Empty);

            connection.Execute($"SELECT TOP 0 {allPropertiesExceptKeyAndComputedString} INTO {tempToBeInserted} FROM {tableName} target WITH(NOLOCK);", null, transaction);

            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout;
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = tempToBeInserted;
                bulkCopy.WriteToServer(ToDataTable(data, tableName, allPropertiesExceptKeyAndComputed).CreateDataReader());
            }

            var table = string.Join(", ", keyProperties.Select(k => $"[{k.Name }] bigint"));
            var joinOn = string.Join(" AND ", keyProperties.Select(k => $"target.[{k.Name }] = ins.[{k.Name }]"));
            return connection.Query<T>($@"
                DECLARE {tempInsertedWithIdentity} TABLE ({table})
                INSERT INTO {tableName}({allPropertiesExceptKeyAndComputedString}) 
                OUTPUT {keyPropertiesInsertedString} INTO {tempInsertedWithIdentity} ({keyPropertiesString})
                SELECT {allPropertiesExceptKeyAndComputedString} FROM {tempToBeInserted}

                SELECT {allPropertiesString}
                FROM {tableName} target INNER JOIN {tempInsertedWithIdentity} ins ON {joinOn}

                DROP TABLE {tempToBeInserted};", null, transaction);
        }

        /// <summary>
        /// Inserts entities into table <typeparamref name="T"/>s (by default) asynchronously.
        /// </summary>
        /// <typeparam name="T">The type being inserted.</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="data">Entities to insert</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="batchSize">Number of bulk items inserted together, 0 (the default) if all</param>
        /// <param name="bulkCopyTimeout">Number of seconds before bulk command execution timeout, 30 (the default)</param>
        public static async Task BulkInsertAsync<T>(this SqlConnection connection, IEnumerable<T> data, SqlTransaction transaction = null, int batchSize = 0, int bulkCopyTimeout = 30)
        {
            var type = typeof(T);
            var tableName = TableMapper.GetTableName(type);
            var allProperties = PropertiesCache.TypePropertiesCache(type);
            var keyProperties = PropertiesCache.KeyPropertiesCache(type);
            var computedProperties = PropertiesCache.ComputedPropertiesCache(type);
            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();
            var allPropertiesExceptKeyAndComputedString = GetColumnsStringSqlServer(allPropertiesExceptKeyAndComputed);
            var tempToBeInserted = $"#{tableName}_TempInsert".Replace(".", string.Empty);

            await connection.ExecuteAsync($@"SELECT TOP 0 {allPropertiesExceptKeyAndComputedString} INTO {tempToBeInserted} FROM {tableName} target WITH(NOLOCK);", null, transaction);

            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout;
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = tempToBeInserted;
                await bulkCopy.WriteToServerAsync(ToDataTable(data, tableName, allPropertiesExceptKeyAndComputed).CreateDataReader());
            }

            await connection.ExecuteAsync($@"
                INSERT INTO {tableName}({allPropertiesExceptKeyAndComputedString}) 
                SELECT {allPropertiesExceptKeyAndComputedString} FROM {tempToBeInserted}

                DROP TABLE {tempToBeInserted};", null, transaction);
        }

        /// <summary>
        /// Inserts entities into table <typeparamref name="T"/>s (by default) asynchronously and returns inserted entities.
        /// </summary>
        /// <typeparam name="T">The type being inserted.</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="data">Entities to insert</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="batchSize">Number of bulk items inserted together, 0 (the default) if all</param>
        /// <param name="bulkCopyTimeout">Number of seconds before bulk command execution timeout, 30 (the default)</param>
        /// <returns>Inserted entities</returns>
        public static async Task<IEnumerable<T>> BulkInsertAndSelectAsync<T>(this SqlConnection connection, IEnumerable<T> data, SqlTransaction transaction = null, int batchSize = 0, int bulkCopyTimeout = 30)
        {
            var type = typeof(T);
            var tableName = TableMapper.GetTableName(type);
            var allProperties = PropertiesCache.TypePropertiesCache(type);
            var keyProperties = PropertiesCache.KeyPropertiesCache(type);
            var computedProperties = PropertiesCache.ComputedPropertiesCache(type);
            if (keyProperties.Count == 0)
            {
                var dataList = data.ToList();
                await connection.BulkInsertAsync(dataList, transaction, batchSize, bulkCopyTimeout);
                return dataList;
            }

            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            var keyPropertiesString = GetColumnsStringSqlServer(keyProperties);
            var keyPropertiesInsertedString = GetColumnsStringSqlServer(keyProperties, "inserted.");
            var allPropertiesExceptKeyAndComputedString = GetColumnsStringSqlServer(allPropertiesExceptKeyAndComputed);
            var allPropertiesString = GetColumnsStringSqlServer(allProperties, "target.");

            var tempToBeInserted = $"#{tableName}_TempInsert".Replace(".", string.Empty);
            var tempInsertedWithIdentity = $"@{tableName}_TempInserted".Replace(".", string.Empty);

            await connection.ExecuteAsync($@"SELECT TOP 0 {allPropertiesExceptKeyAndComputedString} INTO {tempToBeInserted} FROM {tableName} target WITH(NOLOCK);", null, transaction);

            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout;
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = tempToBeInserted;
                await bulkCopy.WriteToServerAsync(ToDataTable(data, tableName, allPropertiesExceptKeyAndComputed).CreateDataReader());
            }

            var table = string.Join(", ", keyProperties.Select(k => $"[{k.Name }] bigint"));
            var joinOn = string.Join(" AND ", keyProperties.Select(k => $"target.[{k.Name }] = ins.[{k.Name }]"));
            return await connection.QueryAsync<T>($@"
                DECLARE {tempInsertedWithIdentity} TABLE ({table})
                INSERT INTO {tableName}({allPropertiesExceptKeyAndComputedString}) 
                OUTPUT {keyPropertiesInsertedString} INTO {tempInsertedWithIdentity} ({keyPropertiesString})
                SELECT {allPropertiesExceptKeyAndComputedString} FROM {tempToBeInserted}

                SELECT {allPropertiesString}
                FROM {tableName} target INNER JOIN {tempInsertedWithIdentity} ins ON {joinOn}

                DROP TABLE {tempToBeInserted};", null, transaction);
        }

        private static string GetColumnsStringSqlServer(IEnumerable<PropertyInfo> properties, string tablePrefix = null)
        {
            return string.Join(", ", properties.Select(property => $"{tablePrefix}[{property.Name}]"));
        }

        private static DataTable ToDataTable<T>(IEnumerable<T> data, string tableName, IList<PropertyInfo> properties)
        {
            var dataTable = new DataTable(tableName);
            foreach (var prop in properties)
            {
                dataTable.Columns.Add(prop.Name);
            }

            foreach (var item in data)
            {
                var values = new object[properties.Count];
                for (var i = 0; i < properties.Count; i++)
                {
                    values[i] = properties[i].GetValue(item, null);
                }

                dataTable.Rows.Add(values);
            }

            return dataTable;
        }
    }
}