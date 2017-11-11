using System;
using System.Collections.Concurrent;
using System.Data;
using System.Linq;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Dapper.Bulk.Tests")]

namespace Dapper.Bulk
{
    public static class TableMapper
    {
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> TableNames = new ConcurrentDictionary<RuntimeTypeHandle, string>();

        private static string _prefix = string.Empty;
        private static string _sufux = "s";

        public static void SetupConvention(string tablePrefix, string tableSufix)
        {
            if (TableNames.Count > 0)
            {
                throw new InvalidConstraintException("TableMapper.SetupConvention called after usage.");
            }

            _prefix = tablePrefix;
            _sufux = tableSufix;
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
                name = _prefix + name + _sufux;
            }

            TableNames[type.TypeHandle] = name;
            return name;
        }
    }
}