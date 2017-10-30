using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Threading.Tasks;

namespace Dapper.Bulk
{
    internal interface IBulkInsert
    {
        void BulkInsert<T>(
            IDbConnection connection,
            IDbTransaction transaction,
            IReadOnlyCollection<T> data,
            int batchSize,
            int bulkCopyTimeout,
            string tableName,
            IReadOnlyCollection<PropertyInfo> allProperties,
            IReadOnlyCollection<PropertyInfo> keyProperties,
            IReadOnlyCollection<PropertyInfo> computedProperties);

        IEnumerable<T> BulkInsertAndSelect<T>(
            IDbConnection connection,
            IDbTransaction transaction,
            IReadOnlyCollection<T> data,
            int batchSize,
            int bulkCopyTimeout,
            string tableName,
            IReadOnlyCollection<PropertyInfo> allProperties,
            IReadOnlyCollection<PropertyInfo> keyProperties,
            IReadOnlyCollection<PropertyInfo> computedProperties);
        
        Task BulkInsertAsync<T>(
            IDbConnection connection,
            IDbTransaction transaction,
            IReadOnlyCollection<T> data,
            int batchSize,
            int bulkCopyTimeout,
            string tableName,
            IReadOnlyCollection<PropertyInfo> allProperties,
            IReadOnlyCollection<PropertyInfo> keyProperties,
            IReadOnlyCollection<PropertyInfo> computedProperties);

        Task<IEnumerable<T>> BulkInsertAsyncAndSelect<T>(
            IDbConnection connection,
            IDbTransaction transaction,
            IReadOnlyCollection<T> data,
            int batchSize,
            int bulkCopyTimeout,
            string tableName,
            IReadOnlyCollection<PropertyInfo> allProperties,
            IReadOnlyCollection<PropertyInfo> keyProperties,
            IReadOnlyCollection<PropertyInfo> computedProperties);
    }
}
