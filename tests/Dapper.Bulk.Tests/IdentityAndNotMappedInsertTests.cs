using FluentAssertions;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dapper.Bulk.Tests;

public class IdentityAndNotMappedInsertTests : SqlServerTestSuite
{
    private class TestSublass
    {

    }

    private class IdentityAndNotMappedTest
    {
        [Key]
        public int IdKey { get; set; }

        public string Name { get; set; }

        public virtual TestSublass TestSublass { get; set; }

        [NotMapped]
        public int Ignored { get; set; }
    }


    [Fact]
    public void InsertBulk()
    {
        var data = new List<IdentityAndNotMappedTest>();
        for (var i = 0; i < 10; i++)
        {
            data.Add(new IdentityAndNotMappedTest { Name = Guid.NewGuid().ToString() ,Ignored=2});
        }

        using var connection = GetConnection();
        connection.Open();
        var inserted = connection.BulkInsertAndSelect(data).ToList();
        for (var i = 0; i < data.Count; i++)
        {
            IsValidInsert(inserted[i], data[i]);
        }
    }

    [Fact]
    public void InsertSingle()
    {
        var item = new IdentityAndNotMappedTest { Name = Guid.NewGuid().ToString(), Ignored = 2 };
        using var connection = GetConnection();
        connection.Open();
        var inserted = connection.BulkInsertAndSelect(new List<IdentityAndNotMappedTest> { item }).First();
        IsValidInsert(inserted, item);
    }

    [Fact]
    public async Task InsertSingleAsync()
    {
        var item = new IdentityAndNotMappedTest { Name = Guid.NewGuid().ToString(), Ignored = 2 };
        using var connection = GetConnection();
        connection.Open();
        var inserted = (await connection.BulkInsertAndSelectAsync(new List<IdentityAndNotMappedTest> { item })).First();
        IsValidInsert(inserted, item);
    }

    [Fact]
    public void InsertSingleTransaction()
    {
        var item = new IdentityAndNotMappedTest { Name = Guid.NewGuid().ToString(), Ignored = 2 };
        using var connection = GetConnection();
        connection.Open();
        using var transaction = connection.BeginTransaction();
        var inserted = connection.BulkInsertAndSelect(new List<IdentityAndNotMappedTest> { item }, transaction).First();
        IsValidInsert(inserted, item);
    }

    private static void IsValidInsert(IdentityAndNotMappedTest inserted, IdentityAndNotMappedTest toBeInserted)
    {
        inserted.IdKey.Should().BePositive();
        inserted.Ignored.Should().NotBe(toBeInserted.Ignored);
        inserted.Name.Should().Be(toBeInserted.Name);
        inserted.TestSublass.Should().BeNull();
    }
}
