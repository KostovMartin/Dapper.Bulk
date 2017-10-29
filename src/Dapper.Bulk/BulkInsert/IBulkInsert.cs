using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Threading.Tasks;

namespace Dapper.Bulk
{
    internal interface IBulkInsert
    {
        IEnumerable<T> BulkInsert<T>(
            IDbConnection connection,
            IDbTransaction transaction,
            IReadOnlyCollection<T> data,
            string tableName,
            IReadOnlyCollection<PropertyInfo> allProperties,
            IReadOnlyCollection<PropertyInfo> keyProperties,
            IReadOnlyCollection<PropertyInfo> computedProperties);

        Task<IEnumerable<T>> BulkInsertAsync<T>(
            IDbConnection connection,
            IDbTransaction transaction,
            IReadOnlyCollection<T> data,
            string tableName,
            IReadOnlyCollection<PropertyInfo> allProperties,
            IReadOnlyCollection<PropertyInfo> keyProperties,
            IReadOnlyCollection<PropertyInfo> computedProperties);
    }
}
