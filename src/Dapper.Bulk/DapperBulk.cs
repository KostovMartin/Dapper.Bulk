using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Dapper.Bulk
{
    public static partial class DapperBulk
    {
        public static IEnumerable<T> BulkInsert<T>(this SqlConnection connection, IEnumerable<T> data, SqlTransaction transaction = null)
        {
            var type = typeof(T);
            var tableName = Cache.GetTableName(type);
            var allProperties = Cache.TypePropertiesCache(type);
            var keyProperties = Cache.KeyPropertiesCache(type);
            var computedProperties = Cache.ComputedPropertiesCache(type);
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
                bulkCopy.WriteToServer(data.ToDataTable().CreateDataReader());
            }

            var reader = connection.Query<T>($@"
                DECLARE {tempInsertedWithIdentity} TABLE ( ID bigint )
                INSERT INTO {tableName}({allPropertiesExceptKeyAndComputedString}) 
                OUTPUT {keyPropertiesInsertedString} INTO {tempInsertedWithIdentity} ({keyPropertiesString})
                SELECT {allPropertiesExceptKeyAndComputedString} FROM {tempToBeInserted}

                SELECT {allPropertiesString}
                FROM {tableName} target INNER JOIN {tempInsertedWithIdentity} ins ON target.id = ins.id

                DROP TABLE {tempToBeInserted};", null, transaction);

            return reader;         
        }

        public static async Task<IEnumerable<T>> BulkInsertAsync<T>(this SqlConnection connection, IEnumerable<T> data, SqlTransaction transaction = null)
        {

            var type = typeof(T);
            var tableName = Cache.GetTableName(type);
            var allProperties = Cache.TypePropertiesCache(type);
            var keyProperties = Cache.KeyPropertiesCache(type);
            var computedProperties = Cache.ComputedPropertiesCache(type);
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
                await bulkCopy.WriteToServerAsync(data.ToDataTable().CreateDataReader());
            }

            var reader = await connection.QueryAsync<T>($@"
                DECLARE {tempInsertedWithIdentity} TABLE ( ID bigint )
                INSERT INTO {tableName}({allPropertiesExceptKeyAndComputedString}) 
                OUTPUT {keyPropertiesInsertedString} INTO {tempInsertedWithIdentity} ({keyPropertiesString})
                SELECT {allPropertiesExceptKeyAndComputedString} FROM {tempToBeInserted}

                SELECT {allPropertiesString}
                FROM {tableName} target INNER JOIN {tempInsertedWithIdentity} ins ON target.id = ins.id

                DROP TABLE {tempToBeInserted};", null, transaction);

            return reader;
        }

        private static string GetColumnsStringSqlServer(IEnumerable<PropertyInfo> properties, string tablePrefix = null)
        {
            return string.Join(", ", properties.Select(property => $"{tablePrefix}[{property.Name}]"));
        }
        
        private static DataTable ToDataTable<T>(this IEnumerable<T> data)
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