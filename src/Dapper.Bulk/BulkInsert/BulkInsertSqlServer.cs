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
            IDbTransaction transaction, 
            IReadOnlyCollection<T> data, 
            string tableName,
            IReadOnlyCollection<PropertyInfo> allProperties,
            IReadOnlyCollection<PropertyInfo> keyProperties,
            IReadOnlyCollection<PropertyInfo> computedProperties)
        {
            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            var keyPropertiesString = GetColumnsStringSqlServer(keyProperties);
            var keyPropertiesInsertedString = GetColumnsStringSqlServer(keyProperties, "inserted.");
            var allPropertiesExceptKeyAndComputedString = GetColumnsStringSqlServer(allPropertiesExceptKeyAndComputed);
            var allPropertiesString = GetColumnsStringSqlServer(allProperties, "target.");

            var tempToBeInserted = $"#{tableName}_TempInsert";
            var tempInsertedWithIdentity = $"@{tableName}_TempInserted";

            connection.Execute($"SELECT TOP 0 {allPropertiesExceptKeyAndComputedString} INTO {tempToBeInserted} FROM {tableName} target WITH(NOLOCK);", null, transaction);

            using (var bulkCopy = new SqlBulkCopy(connection as SqlConnection, SqlBulkCopyOptions.Default, transaction as SqlTransaction))
            {
                bulkCopy.DestinationTableName = tempToBeInserted;
                bulkCopy.WriteToServer(ToDataTable(data, tableName, allPropertiesExceptKeyAndComputed).CreateDataReader());
            }

            if (keyProperties.Count == 0)
            {
                connection.Execute($@"
                    INSERT INTO {tableName}({allPropertiesExceptKeyAndComputedString}) 
                    SELECT {allPropertiesExceptKeyAndComputedString} FROM {tempToBeInserted}

                    DROP TABLE {tempToBeInserted};", null, transaction);
                
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
            IDbTransaction transaction,
            IReadOnlyCollection<T> data,
            string tableName,
            IReadOnlyCollection<PropertyInfo> allProperties,
            IReadOnlyCollection<PropertyInfo> keyProperties,
            IReadOnlyCollection<PropertyInfo> computedProperties)
        {
            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            var keyPropertiesString = GetColumnsStringSqlServer(keyProperties);
            var keyPropertiesInsertedString = GetColumnsStringSqlServer(keyProperties, "inserted.");
            var allPropertiesExceptKeyAndComputedString = GetColumnsStringSqlServer(allPropertiesExceptKeyAndComputed);
            var allPropertiesString = GetColumnsStringSqlServer(allProperties, "target.");

            var tempToBeInserted = $"#{tableName}_TempInsert";
            var tempInsertedWithIdentity = $"@{tableName}_TempInserted";

            await connection.ExecuteAsync($@"SELECT TOP 0 {allPropertiesExceptKeyAndComputedString} INTO {tempToBeInserted} FROM {tableName} target WITH(NOLOCK);", null, transaction);

            using (var bulkCopy = new SqlBulkCopy(connection as SqlConnection, SqlBulkCopyOptions.Default, transaction as SqlTransaction))
            {
                bulkCopy.DestinationTableName = tempToBeInserted;
                await bulkCopy.WriteToServerAsync(ToDataTable(data, tableName, allPropertiesExceptKeyAndComputed).CreateDataReader());
            }

            if (keyProperties.Count == 0)
            {
                await connection.ExecuteAsync($@"
                    INSERT INTO {tableName}({allPropertiesExceptKeyAndComputedString}) 
                    SELECT {allPropertiesExceptKeyAndComputedString} FROM {tempToBeInserted}

                    DROP TABLE {tempToBeInserted};", null, transaction);
                
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
