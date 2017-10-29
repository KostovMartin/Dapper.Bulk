using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Dapper.Bulk
{
    internal class BulkInsertSqlServer : IBulkInsert
    {
        public IEnumerable<T> BulkInsert<T>(
            IDbConnection connection, 
            IEnumerable<T> data, 
            string tableName, 
            IList<PropertyInfo> allProperties,
            IList<PropertyInfo> keyProperties,
            IList<PropertyInfo> computedProperties,
            IDbTransaction transaction)
        {
            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            var keyPropertiesString = GetColumnsStringSqlServer(keyProperties);
            var keyPropertiesInsertedString = GetColumnsStringSqlServer(keyProperties, "inserted.");
            var allPropertiesExceptKeyAndComputedString = GetColumnsStringSqlServer(allPropertiesExceptKeyAndComputed);
            var allPropertiesString = GetColumnsStringSqlServer(allProperties, "target.");

            var tempToBeInserted = $"#{tableName}_TempInsert";
            var tempInsertedWithIdentity = $"@{tableName}_TempInserted";

            connection.Execute($"SELECT TOP 0 {allPropertiesString} INTO {tempToBeInserted} FROM {tableName} target WITH(NOLOCK);", null, transaction);

            using (var bulkCopy = new SqlBulkCopy(connection as SqlConnection, SqlBulkCopyOptions.Default, transaction as SqlTransaction))
            {
                bulkCopy.DestinationTableName = tempToBeInserted;
                bulkCopy.WriteToServer(ToDataTable(data).CreateDataReader());
            }

            if (keyProperties.Count == 0)
            {
                if (computedProperties.Count > 0)
                {
                    throw new NotSupportedException("No identity and Computed Property is not supported.");
                }

                var inserted = connection.Execute($@"
                    INSERT INTO {tableName}({allPropertiesExceptKeyAndComputedString}) 
                    SELECT {allPropertiesExceptKeyAndComputedString} FROM {tempToBeInserted}

                    DROP TABLE {tempToBeInserted};", null, transaction);

                if (data.Count() != inserted)
                {
                    throw new ArgumentException("Bulk Insert failed.");
                }

                return data;
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


        public async Task<IEnumerable<T>> BulkInsertAsync<T>(
            IDbConnection connection,
            IEnumerable<T> data,
            string tableName,
            IList<PropertyInfo> allProperties,
            IList<PropertyInfo> keyProperties,
            IList<PropertyInfo> computedProperties,
            IDbTransaction transaction)
        {
            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            var keyPropertiesString = GetColumnsStringSqlServer(keyProperties);
            var keyPropertiesInsertedString = GetColumnsStringSqlServer(keyProperties, "inserted.");
            var allPropertiesExceptKeyAndComputedString = GetColumnsStringSqlServer(allPropertiesExceptKeyAndComputed);
            var allPropertiesString = GetColumnsStringSqlServer(allProperties, "target.");

            var tempToBeInserted = $"#{tableName}_TempInsert";
            var tempInsertedWithIdentity = $"@{tableName}_TempInserted";

            await connection.ExecuteAsync($@"SELECT TOP 0 {allPropertiesString} INTO {tempToBeInserted} FROM {tableName} target WITH(NOLOCK);", null, transaction);

            using (var bulkCopy = new SqlBulkCopy(connection as SqlConnection, SqlBulkCopyOptions.Default, transaction as SqlTransaction))
            {
                bulkCopy.DestinationTableName = tempToBeInserted;
                await bulkCopy.WriteToServerAsync(ToDataTable(data).CreateDataReader());
            }

            if (keyProperties.Count == 0)
            {
                if (computedProperties.Count > 0)
                {
                    throw new NotSupportedException("No identity and Computed Property is not supported.");
                }

                var inserted = await connection.ExecuteAsync($@"
                    INSERT INTO {tableName}({allPropertiesExceptKeyAndComputedString}) 
                    SELECT {allPropertiesExceptKeyAndComputedString} FROM {tempToBeInserted}

                    DROP TABLE {tempToBeInserted};", null, transaction);

                if (data.Count() != inserted)
                {
                    throw new ArgumentException("Bulk Insert failed.");
                }

                return data;
            }

            var table = string.Join(", ", keyProperties.Select(k => $"[{k.Name }] bigint"));
            var joinOn = string.Join(" AND ", keyProperties.Select(k => $"target.[{k.Name }] = ins.[{k.Name }]"));
            var reader = await connection.QueryAsync<T>($@"
                DECLARE {tempInsertedWithIdentity} TABLE ({table})
                INSERT INTO {tableName}({allPropertiesExceptKeyAndComputedString}) 
                OUTPUT {keyPropertiesInsertedString} INTO {tempInsertedWithIdentity} ({keyPropertiesString})
                SELECT {allPropertiesExceptKeyAndComputedString} FROM {tempToBeInserted}

                SELECT {allPropertiesString}
                FROM {tableName} target INNER JOIN {tempInsertedWithIdentity} ins ON {joinOn}

                DROP TABLE {tempToBeInserted};", null, transaction);

            return reader;
        }

        private static string GetColumnsStringSqlServer(IEnumerable<PropertyInfo> properties, string tablePrefix = null)
        {
            return string.Join(", ", properties.Select(property => $"{tablePrefix}[{property.Name}]"));
        }

        private static DataTable ToDataTable<T>(IEnumerable<T> data)
        {
            var dataTable = new DataTable(typeof(T).Name);
            var Props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (var prop in Props)
            {
                dataTable.Columns.Add(prop.Name);
            }

            foreach (var item in data)
            {
                var values = new object[Props.Length];
                for (var i = 0; i < Props.Length; i++)
                {
                    values[i] = Props[i].GetValue(item, null);
                }
                dataTable.Rows.Add(values);
            }

            return dataTable;
        }
    }
}
