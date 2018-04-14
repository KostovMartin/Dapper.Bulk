using System;
using System.Collections.Concurrent;
using System.Data;
using System.Linq;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Dapper.Bulk.Tests")]

namespace Dapper.Bulk
{
    /// <summary>
    /// Used to store table names
    /// </summary>
    public static class TableMapper
    {
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> TableNames = new ConcurrentDictionary<RuntimeTypeHandle, string>();

        private static string _prefix = string.Empty;
        private static string _suffix = "s";

        /// <summary>
        /// Used to setup custom table conventions.
        /// </summary>
        /// <param name="tablePrefix">table name prefix</param>
        /// <param name="tableSuffix">table name suffix</param>
        public static void SetupConvention(string tablePrefix, string tableSuffix)
        {
            if (TableNames.Count > 0)
            {
                throw new InvalidConstraintException("TableMapper.SetupConvention called after usage.");
            }

            _prefix = tablePrefix;
            _suffix = tableSuffix;
            
            TableNames.Clear();
        }
        
        internal static string GetTableName(Type type)
        {
            if (TableNames.TryGetValue(type.TypeHandle, out var name))
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
                name = type.IsInterface && type.Name.StartsWith("I") 
                    ? type.Name.Substring(1) 
                    : type.Name;
                name = _prefix + name + _suffix;
            }

            TableNames[type.TypeHandle] = name;
            return name;
        }
    }
}
