using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Dapper.Bulk
{
    public static class DapperBulk
    {
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> TableNames = new ConcurrentDictionary<RuntimeTypeHandle, string>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> KeyProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> TypeProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> ComputedProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly IBulkInsert BulkInsertSqlServer = new BulkInsertSqlServer();

        public static void BulkInsert<T>(this IDbConnection connection, IEnumerable<T> data, IDbTransaction transaction = null)
        {
            var adapter = GetDbAdapter(connection);
            var type = typeof(T);
            var tableName = GetTableName(type);
            var allProperties = TypePropertiesCache(type);
            var keyProperties = KeyPropertiesCache(type);
            var computedProperties = ComputedPropertiesCache(type);
            adapter.BulkInsert(connection, transaction, data.ToList(), tableName, allProperties, keyProperties, computedProperties);
        }

        public static IEnumerable<T> BulkInsertAndSelect<T>(this IDbConnection connection, IEnumerable<T> data, IDbTransaction transaction = null)
        {
            var adapter = GetDbAdapter(connection);
            var type = typeof(T);
            var tableName = GetTableName(type);
            var allProperties = TypePropertiesCache(type);
            var keyProperties = KeyPropertiesCache(type);
            var computedProperties = ComputedPropertiesCache(type);
            return adapter.BulkInsertAndSelect(connection, transaction, data.ToList(), tableName, allProperties, keyProperties, computedProperties);  
        }

        public static Task BulkInsertAsync<T>(this IDbConnection connection, IEnumerable<T> data, IDbTransaction transaction = null)
        {
            var adapter = GetDbAdapter(connection);
            var type = typeof(T);
            var tableName = GetTableName(type);
            var allProperties = TypePropertiesCache(type);
            var keyProperties = KeyPropertiesCache(type);
            var computedProperties = ComputedPropertiesCache(type);
            return adapter.BulkInsertAsync(connection, transaction, data.ToList(), tableName, allProperties, keyProperties, computedProperties);
        }

        public static Task<IEnumerable<T>> BulkInsertAndSelectAsync<T>(this IDbConnection connection, IEnumerable<T> data, IDbTransaction transaction = null)
        {
            var inserter = GetDbAdapter(connection);
            var type = typeof(T);
            var tableName = GetTableName(type);
            var allProperties = TypePropertiesCache(type);
            var keyProperties = KeyPropertiesCache(type);
            var computedProperties = ComputedPropertiesCache(type);
            return inserter.BulkInsertAsyncAndSelect(connection, transaction, data.ToList(), tableName, allProperties, keyProperties, computedProperties);
        }

        private static IBulkInsert GetDbAdapter(IDbConnection connection)
        {
            if (connection is SqlConnection)
            {
                return BulkInsertSqlServer;
            }

            throw new NotSupportedException();
        }

        private static string GetTableName(Type type)
        {
            if (TableNames.TryGetValue(type.TypeHandle, out string name))
            {
                return name;
            }

            var tableAttr = type.GetCustomAttributes(false).SingleOrDefault(attr => attr.GetType().Name == "TableAttribute") as dynamic;
            if (tableAttr != null)
            {
                name = tableAttr.Name;
            }
            else
            {
                name = type.Name + "s";
                if (type.IsInterface && name.StartsWith("I"))
                {
                    name = name.Substring(1);
                }
            }

            TableNames[type.TypeHandle] = name;
            return name;
        }

        private static List<PropertyInfo> TypePropertiesCache(Type type)
        {
            if (TypeProperties.TryGetValue(type.TypeHandle, out IEnumerable<PropertyInfo> cachedProps))
            {
                return cachedProps.ToList();
            }

            var properties = type.GetProperties().ToArray();
            TypeProperties[type.TypeHandle] = properties;
            return properties.ToList();
        }

        private static List<PropertyInfo> KeyPropertiesCache(Type type)
        {
            if (KeyProperties.TryGetValue(type.TypeHandle, out IEnumerable<PropertyInfo> cachedProps))
            {
                return cachedProps.ToList();
            }

            var allProperties = TypePropertiesCache(type);
            var keyProperties = allProperties.Where(p => p.GetCustomAttributes(true).Any(a => a.GetType().Name == "KeyAttribute")).ToList();

            if (keyProperties.Count == 0)
            {
                var idProp = allProperties.Find(p => string.Equals(p.Name, "id", StringComparison.CurrentCultureIgnoreCase));
                if (idProp != null)
                {
                    keyProperties.Add(idProp);
                }
            }

            KeyProperties[type.TypeHandle] = keyProperties;
            return keyProperties;
        }

        private static List<PropertyInfo> ComputedPropertiesCache(Type type)
        {
            if (ComputedProperties.TryGetValue(type.TypeHandle, out IEnumerable<PropertyInfo> cachedProps))
            {
                return cachedProps.ToList();
            }

            var computedProperties = TypePropertiesCache(type).Where(p => p.GetCustomAttributes(true).Any(a => a.GetType().Name == "ComputedAttribute")).ToList();
            ComputedProperties[type.TypeHandle] = computedProperties;
            return computedProperties;
        }
    }
}