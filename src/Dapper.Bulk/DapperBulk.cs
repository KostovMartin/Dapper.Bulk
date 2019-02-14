using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("Dapper.Bulk.Tests")]

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
            var columns = PropertiesCache.GetColumnNamesCache(type);

            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();
            var allPropertiesExceptKeyAndComputedString = GetColumnsStringSqlServer(allPropertiesExceptKeyAndComputed, columns);
            var tempToBeInserted = $"#TempInsert_{tableName}".Replace(".", string.Empty);

            connection.Execute($@"SELECT TOP 0 {allPropertiesExceptKeyAndComputedString} INTO {tempToBeInserted} FROM {FormatTableName(tableName)} target WITH(NOLOCK);", null, transaction);

            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout;
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = tempToBeInserted;
                bulkCopy.WriteToServer(ToDataTable(data, allPropertiesExceptKeyAndComputed).CreateDataReader());
            }

            connection.Execute($@"
                INSERT INTO {FormatTableName(tableName)}({allPropertiesExceptKeyAndComputedString}) 
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
            var columns = PropertiesCache.GetColumnNamesCache(type);

            if (keyProperties.Count == 0)
            {
                var dataList = data.ToList();
                connection.BulkInsert(dataList, transaction, batchSize, bulkCopyTimeout);
                return dataList;
            }

            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            var keyPropertiesString = GetColumnsStringSqlServer(keyProperties,columns);
            var keyPropertiesInsertedString = GetColumnsStringSqlServer(keyProperties, columns, "inserted.");
            var allPropertiesExceptKeyAndComputedString = GetColumnsStringSqlServer(allPropertiesExceptKeyAndComputed, columns);
            var allPropertiesString = GetColumnsStringSqlServer(allProperties, columns, "target.");

            var tempToBeInserted = $"#TempInsert_{tableName}".Replace(".", string.Empty);
            var tempInsertedWithIdentity = $"@TempInserted_{tableName}".Replace(".", string.Empty);

            connection.Execute($"SELECT TOP 0 {allPropertiesExceptKeyAndComputedString} INTO {tempToBeInserted} FROM {FormatTableName(tableName)} target WITH(NOLOCK);", null, transaction);

            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout;
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = tempToBeInserted;
                bulkCopy.WriteToServer(ToDataTable(data, allPropertiesExceptKeyAndComputed).CreateDataReader());
            }

            var table = string.Join(", ", keyProperties.Select(k => $"[{k.Name }] bigint"));
            var joinOn = string.Join(" AND ", keyProperties.Select(k => $"target.[{k.Name }] = ins.[{k.Name }]"));
            return connection.Query<T>($@"
                DECLARE {tempInsertedWithIdentity} TABLE ({table})
                INSERT INTO {FormatTableName(tableName)}({allPropertiesExceptKeyAndComputedString}) 
                OUTPUT {keyPropertiesInsertedString} INTO {tempInsertedWithIdentity} ({keyPropertiesString})
                SELECT {allPropertiesExceptKeyAndComputedString} FROM {tempToBeInserted}

                SELECT {allPropertiesString}
                FROM {FormatTableName(tableName)} target INNER JOIN {tempInsertedWithIdentity} ins ON {joinOn}

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
            var columns = PropertiesCache.GetColumnNamesCache(type);

            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();
            var allPropertiesExceptKeyAndComputedString = GetColumnsStringSqlServer(allPropertiesExceptKeyAndComputed,columns);
            var tempToBeInserted = $"#TempInsert_{tableName}".Replace(".", string.Empty);

            await connection.ExecuteAsync($@"SELECT TOP 0 {allPropertiesExceptKeyAndComputedString} INTO {tempToBeInserted} FROM {FormatTableName(tableName)} target WITH(NOLOCK);", null, transaction);

            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout;
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = tempToBeInserted;
                await bulkCopy.WriteToServerAsync(ToDataTable(data, allPropertiesExceptKeyAndComputed).CreateDataReader());
            }

            await connection.ExecuteAsync($@"
                INSERT INTO {FormatTableName(tableName)}({allPropertiesExceptKeyAndComputedString}) 
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
            var columns = PropertiesCache.GetColumnNamesCache(type);

            if (keyProperties.Count == 0)
            {
                var dataList = data.ToList();
                await connection.BulkInsertAsync(dataList, transaction, batchSize, bulkCopyTimeout);
                return dataList;
            }

            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            var keyPropertiesString = GetColumnsStringSqlServer(keyProperties,columns);
            var keyPropertiesInsertedString = GetColumnsStringSqlServer(keyProperties,columns, "inserted.");
            var allPropertiesExceptKeyAndComputedString = GetColumnsStringSqlServer(allPropertiesExceptKeyAndComputed,columns);
            var allPropertiesString = GetColumnsStringSqlServer(allProperties, columns, "target.");

            var tempToBeInserted = $"#TempInsert_{tableName}".Replace(".", string.Empty);
            var tempInsertedWithIdentity = $"@TempInserted_{tableName}".Replace(".", string.Empty);

            await connection.ExecuteAsync($@"SELECT TOP 0 {allPropertiesExceptKeyAndComputedString} INTO {tempToBeInserted} FROM {FormatTableName(tableName)} target WITH(NOLOCK);", null, transaction);

            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout;
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = tempToBeInserted;
                await bulkCopy.WriteToServerAsync(ToDataTable(data, allPropertiesExceptKeyAndComputed).CreateDataReader());
            }

            var table = string.Join(", ", keyProperties.Select(k => $"[{k.Name }] bigint"));
            var joinOn = string.Join(" AND ", keyProperties.Select(k => $"target.[{k.Name }] = ins.[{k.Name }]"));
            return await connection.QueryAsync<T>($@"
                DECLARE {tempInsertedWithIdentity} TABLE ({table})
                INSERT INTO {FormatTableName(tableName)}({allPropertiesExceptKeyAndComputedString}) 
                OUTPUT {keyPropertiesInsertedString} INTO {tempInsertedWithIdentity} ({keyPropertiesString})
                SELECT {allPropertiesExceptKeyAndComputedString} FROM {tempToBeInserted}

                SELECT {allPropertiesString}
                FROM {FormatTableName(tableName)} target INNER JOIN {tempInsertedWithIdentity} ins ON {joinOn}

                DROP TABLE {tempToBeInserted};", null, transaction);
        }

        private static string GetColumnsStringSqlServer(IEnumerable<PropertyInfo> properties, IReadOnlyDictionary<string, string> columnNames, string tablePrefix = null)
        {
            if (tablePrefix == "target.")
            {
                return string.Join(", ", properties.Select(property => $"{tablePrefix}[{columnNames[property.Name]}] as [{property.Name}] "));
            }

            return string.Join(", ", properties.Select(property => $"{tablePrefix}[{columnNames[property.Name]}] "));
        }
        
        private static DataTable ToDataTable<T>(IEnumerable<T> data, IList<PropertyInfo> properties)
        {
            var dataTable = new DataTable();
            foreach (var prop in properties)
            {
                dataTable.Columns.Add(prop.Name);
            }

            var typeCasts = new Type[properties.Count];
            for (var i = 0; i < properties.Count; i++)
            {
                var isEnum = properties[i].PropertyType.IsEnum;
                if (isEnum)
                {
                    typeCasts[i] = Enum.GetUnderlyingType(properties[i].PropertyType);
                }
                else
                {
                    typeCasts[i] = null;
                }
            }

            foreach (var item in data)
            {
                var values = new object[properties.Count];
                for (var i = 0; i < properties.Count; i++)
                {
                    var value = properties[i].GetValue(item, null);
                    var castToType = typeCasts[i];
                    values[i] = castToType == null ? value : Convert.ChangeType(value, castToType);
                }

                dataTable.Rows.Add(values);
            }

            return dataTable;
        }

        internal static string FormatTableName(string table)
        {
            if (string.IsNullOrEmpty(table))
            {
                return table;
            }

            var parts = table.Split('.');

            if (parts.Length == 1)
            {
                return $"[{table}]";
            }

            return $"[{parts[0]}].[{parts[1]}]";
        }
    }
}
