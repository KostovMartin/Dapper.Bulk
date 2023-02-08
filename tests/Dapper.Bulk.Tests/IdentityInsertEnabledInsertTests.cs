using FluentAssertions;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dapper.Bulk.Tests;

public class IdentityInsertEnabledInsertTests : SqlServerTestSuite
{
    private class IdentityInsertEnabledTest
    {
        [Key]
        public int IdKey { get; set; }

        public string Name { get; set; }
    }

    [Fact]
    public void InsertBulk()
    {
        var data = new List<IdentityInsertEnabledTest>();
        for (var i = 1; i <= 10; i++)
        {
            data.Add(new IdentityInsertEnabledTest { IdKey = -1*i, Name = Guid.NewGuid().ToString() });
        }

        using var connection = GetConnection();
        connection.Open();
        var inserted = connection.BulkInsertAndSelect(data, identityInsert: true).ToList();
        for (var i = 0; i < data.Count; i++)
        {
            IsValidInsert(inserted[i], data[i]);
        }
    }
    
    [Fact]
    public void InsertSingle()
    {
        var item = new IdentityInsertEnabledTest { IdKey = -123151, Name = Guid.NewGuid().ToString() };
        using var connection = GetConnection();
        connection.Open();
        var inserted = connection.BulkInsertAndSelect(new List<IdentityInsertEnabledTest> { item }, identityInsert: true).First();
        IsValidInsert(inserted, item);
    }
    
    [Fact]
    public async Task InsertSingleAsync()
    {
        var item = new IdentityInsertEnabledTest { IdKey = -123152, Name = Guid.NewGuid().ToString() };
        using var connection = GetConnection();
        connection.Open();
        var inserted = (await connection.BulkInsertAndSelectAsync(new List<IdentityInsertEnabledTest> { item }, identityInsert: true)).First();
        IsValidInsert(inserted, item);
    }
    
    [Fact]
    public void InsertSingleTransaction()
    {
        var item = new IdentityInsertEnabledTest { IdKey = -123153, Name = Guid.NewGuid().ToString() };
        using var connection = GetConnection();
        connection.Open();
        using var transaction = connection.BeginTransaction();
        var inserted = connection.BulkInsertAndSelect(new List<IdentityInsertEnabledTest> { item }, transaction, identityInsert: true).First();
        IsValidInsert(inserted, item);
    }
    
    private static void IsValidInsert(IdentityInsertEnabledTest inserted, IdentityInsertEnabledTest toBeInserted)
    {
        inserted.IdKey.Should().Be(toBeInserted.IdKey);
        inserted.Name.Should().Be(toBeInserted.Name);
    }
}
