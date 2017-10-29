Dapper.Bulk- bulk inserts for Dapper
===========================================

`Features` 
--------
Dapper.Bulk contains helper methods for bulk inserting.


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
[Table("tblCars")]
public class Car
{
    public int Id { get; set; }
    
    public string Name { get; set; }
}
```

```csharp
public class Car
{
    [Key] // Identity
    public int CarId { get; set; }
    
    public string Name { get; set; }
}
```

```csharp
public class Car
{
    public int Id { get; set; }
    
    [Computed] //will not be part of inserts, but will be returned
    public string Name { get; set; }
}
```