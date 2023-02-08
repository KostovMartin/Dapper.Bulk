using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Dapper.Bulk;

internal static class PropertiesCache
{
    private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> KeyProperties = new();
    private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> TypeProperties = new();
    private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> ComputedProperties = new();
    private static readonly ConcurrentDictionary<RuntimeTypeHandle, IReadOnlyDictionary<string, string>> ColumnNames = new();

    public static List<PropertyInfo> TypePropertiesCache(Type type)
    {
        if (TypeProperties.TryGetValue(type.TypeHandle, out var cachedProps))
        {
            return cachedProps.ToList();
        }

        var properties = type.GetProperties().Where(ValidateProperty).ToList();
        TypeProperties[type.TypeHandle] = properties;
        ColumnNames[type.TypeHandle] = GetColumnNames(properties);

        return properties.ToList();
    }

    public static IReadOnlyDictionary<string, string> GetColumnNamesCache(Type type)
    {
        if (ColumnNames.TryGetValue(type.TypeHandle, out var cachedProps))
        {
            return cachedProps;
        }

        var properties = type.GetProperties().Where(ValidateProperty).ToList();
        TypeProperties[type.TypeHandle] = properties;
        ColumnNames[type.TypeHandle] = GetColumnNames(properties);

        return ColumnNames[type.TypeHandle];
    }

    public static bool ValidateProperty(PropertyInfo prop)
    {
        var result = prop.CanWrite; 
        result = result && (prop.GetSetMethod(true)?.IsPublic ?? false);
        result = result && (!prop.PropertyType.IsClass || prop.PropertyType == typeof(string) || prop.PropertyType == typeof(byte[]));
        result = result && prop.GetCustomAttributes(true).All(a => a.GetType().Name != "NotMappedAttribute");

        var writeAttribute = prop.GetCustomAttributes(true).FirstOrDefault(x => x.GetType().Name == "WriteAttribute");
        if (writeAttribute != null)
        {
            var writeProperty = writeAttribute.GetType().GetProperty("Write");
            if (writeProperty != null && writeProperty.PropertyType == typeof(bool))
            {
                result = result && (bool) writeProperty.GetValue(writeAttribute);
            }
        }

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

    private static IReadOnlyDictionary<string, string> GetColumnNames(IEnumerable<PropertyInfo> props)
    {
        var ret = new Dictionary<string, string>();
        foreach (var prop in props)
        {
            var columnAttr = prop.GetCustomAttributes(false).SingleOrDefault(attr => attr.GetType().Name == "ColumnAttribute") as dynamic;
            // if the column attribute exists, and specifies a column name, use that, otherwise fall back to the property name as the column name
            ret.Add(prop.Name, columnAttr != null ? (string)columnAttr.Name??prop.Name : prop.Name);
        }

        return ret;
    }
}