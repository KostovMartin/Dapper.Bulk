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
* Inserts entities into temp table:

```csharp
connection.BulkInsertIntoTempTable(data,"#mytemptable");
var data2=connection.Query<TypeOfdata>("select * from #mytemptable");
```


`Default Conventions` 
-------

* `TableName` is TypeName + s. When Interface `I` is removed.
* `Key` is Id property (case-insensitive)

`Custom Conventions` 
-------

`TableName` - somewhere before usage call.

```csharp
TableMapper.SetupConvention("tbl", "s")
```

`Attributes` 
-------

We do not rely on specific attributes. This means you can use whatever attributes with following names:
 
* `TableAttribute` - Must have string Name property. Exists in System.ComponentModel.Annotations Nuget.
* `ColumnAttribute` - Must have string Name property. Exists in System.ComponentModel.Annotations Nuget.
* `KeyAttribute` - Marking only attribute. Exists in System.ComponentModel.Annotations Nuget.
* `ComputedAttribute` - Marking only attribute. For fields returned from Db.
* `NotMapped` - Marking only attribute. For ignored fields.

```csharp
// Table Cars by default convention 
public class Car
{
    // Identity by convention
    public int Id { get; set; }
    
    public string Name { get; set; }
	
    public DateTime ManufactureDate { get; set; }
}
```

```csharp

// Supported in v1.2+
public enum CarType : int
{
    Classic = 1,
    Coupe = 2
}

[Table("tblCars")]
public class Car
{
    [Key] // Identity
    public int CarId { get; set; }
    
    public string Name { get; set; }
	
    public CarType CarType { get; set; } //SQL Data Type should match Enum type
	
    [Computed] // Will be ignored for inserts, but the value in database after insert will be returned
    public DateTime ManufactureDate { get; set; }
}
```

```csharp
public class IdentityAndNotMappedTest
{
    [Key]
    public int IdKey { get; set; }

    public string Name { get; set; }

	// Will be ignored for inserts
    public virtual TestSublass TestSublass { get; set; }

    [NotMapped] // Will be ignored for inserts
    public int Ignored { get; set; }
}
```

```csharp
// Supported in v1.4+
private class CustomColumnName
{
    [Key]
    public int IdKey { get; set; }

    [Column("Name_1")] // Will map to SQL column Name_1
    public string Name { get; set; } 

    [Column("Int_Col")] // Will map to SQL column Int_Col
    public int IntCol { get; set; }

    [Column("Long_Col")] // Will map to SQL column Long_Col
    public long LongCol { get; set; }

    [NotMapped] // Will be ignored for inserts
    public int Ignored { get; set; }

    [Write(false)] // Will be ignored for inserts
    public int Ignored { get; set; }
}
```
