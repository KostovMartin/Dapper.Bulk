using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Dapper.Bulk
{
    internal static class PropertiesCache
    {
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> KeyProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> TypeProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> ComputedProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();

        public static List<PropertyInfo> TypePropertiesCache(Type type)
        {
            if (TypeProperties.TryGetValue(type.TypeHandle, out var cachedProps))
            {
                return cachedProps.ToList();
            }
            var properties = type.GetProperties().Where(ValidateProperty);
            TypeProperties[type.TypeHandle] = properties;
            return properties.ToList();
        }

        public static bool ValidateProperty(PropertyInfo prop)
        {
            bool result = prop.CanWrite; 
            result = result && (prop.GetSetMethod(true)?.IsPublic ?? false);
            result = result && (!prop.GetGetMethod()?.IsVirtual ?? false);
            result = result && (!prop.PropertyType.IsClass || prop.PropertyType == typeof(string));
            result = result && prop.GetCustomAttributes(true).All(a => a.GetType().Name != "NotMappedAttribute");

            return result;
        }

        public static List<PropertyInfo> KeyPropertiesCache(Type type)
        {
            if (KeyProperties.TryGetValue(type.TypeHandle, out var cachedProps))
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

        public static List<PropertyInfo> ComputedPropertiesCache(Type type)
        {
            if (ComputedProperties.TryGetValue(type.TypeHandle, out var cachedProps))
            {
                return cachedProps.ToList();
            }

            var computedProperties = TypePropertiesCache(type).Where(p => p.GetCustomAttributes(true).Any(a => a.GetType().Name == "ComputedAttribute")).ToList();
            ComputedProperties[type.TypeHandle] = computedProperties;
            return computedProperties;
        }
    }
}