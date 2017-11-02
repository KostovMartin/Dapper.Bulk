Dapper.Bulk- bulk inserts for Dapper
===========================================

`Features` 
--------
Dapper.Bulk contains helper methods for bulk inserting.

`Download`
--------
<a href="https://www.nuget.org/packages/Dapper.Bulk/" target="_blank">Dapper.Bulk Nuget</a>
```
PM> Install-Package Dapper.Bulk
```

`Usage` 
-------

* Inserts entities, without result for best performance:

```csharp
connection.BulkInsert(data);
```

```csharp
await connection.BulkInsertAsync(data);
```

* Inserts and returns inserted entities:

```csharp
var inserted = connection.BulkInsertAndSelect(data);
```

```csharp
var inserted = await connection.BulkInsertAndSelectAsync(data);
```

`Conventions` 
-------

* `TableName` is TypeName + s. When Interface `I` is removed.
* `Key` is Id property (case-insensitive)

`Attributes` 
-------

We do not rely on specific attributes. This means you can use whatever attributes with following names:
 
* `TableAttribute` - Must have string Name property. Exists in System.ComponentModel.DataAnnotations.Schema
* `KeyAttribute` - Marking only attribute. Exists in System.ComponentModel.DataAnnotations.Schema
* `ComputedAttribute`  - Marking only attribute.

```csharp
// Cars by convention 
public class Car
{
	// Identity by convention
    public int Id { get; set; }
    
    public string Name { get; set; }
	
    public DateTime ManufactureDate { get; set; }
}
```

```csharp
[Table("tblCars")]
public class Car
{
    [Key] // Identity
    public int CarId { get; set; }
    
    public string Name { get; set; }
	
    [Computed] // Will be ignored for inserts, but will be returned
    public DateTime ManufactureDate { get; set; }
}
```