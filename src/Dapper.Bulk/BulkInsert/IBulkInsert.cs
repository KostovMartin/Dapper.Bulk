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
            IEnumerable<T> data,
            string tableName,
            IList<PropertyInfo> allProperties,
            IList<PropertyInfo> keyProperties,
            IList<PropertyInfo> computedProperties,
            IDbTransaction transaction);

        Task<IEnumerable<T>> BulkInsertAsync<T>(
            IDbConnection connection,
            IEnumerable<T> data,
            string tableName,
            IList<PropertyInfo> allProperties,
            IList<PropertyInfo> keyProperties,
            IList<PropertyInfo> computedProperties,
            IDbTransaction transaction);
    }
}
