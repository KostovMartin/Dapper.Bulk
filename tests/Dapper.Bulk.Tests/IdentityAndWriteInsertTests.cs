using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Dapper.Bulk.Tests.Attributes;
using FluentAssertions;
using Xunit;

namespace Dapper.Bulk.Tests;

public class IdentityAndWriteInsertTests : SqlServerTestSuite
{
    private class IdentityAndWriteInsertTest
    {
        [Key]
        public int IdKey { get; set; }

        public string Name { get; set; }

        [Write(false)]
        public int Ignored { get; set; }

        [Write(true)]
        public int NotIgnored { get; set; }
    }

    [Fact]
    public void InsertBulk()
    {
        var data = new List<IdentityAndWriteInsertTests.IdentityAndWriteInsertTest>();
        for (var i = 0; i < 10; i++)
        {
            data.Add(new IdentityAndWriteInsertTests.IdentityAndWriteInsertTest { Name = Guid.NewGuid().ToString(), Ignored = 2, NotIgnored = 5});
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
        var item = new IdentityAndWriteInsertTests.IdentityAndWriteInsertTest { Name = Guid.NewGuid().ToString(), Ignored = 2, NotIgnored = 5 };
        using var connection = GetConnection();
        connection.Open();
        var inserted = connection.BulkInsertAndSelect(new List<IdentityAndWriteInsertTests.IdentityAndWriteInsertTest> { item }).First();
        IsValidInsert(inserted, item);
    }

    [Fact]
    public async Task InsertSingleAsync()
    {
        var item = new IdentityAndWriteInsertTests.IdentityAndWriteInsertTest { Name = Guid.NewGuid().ToString(), Ignored = 2, NotIgnored = 5 };
        using var connection = GetConnection();
        connection.Open();
        var inserted = (await connection.BulkInsertAndSelectAsync(new List<IdentityAndWriteInsertTests.IdentityAndWriteInsertTest> { item })).First();
        IsValidInsert(inserted, item);
    }

    [Fact]
    public void InsertSingleTransaction()
    {
        var item = new IdentityAndWriteInsertTests.IdentityAndWriteInsertTest { Name = Guid.NewGuid().ToString(), Ignored = 2, NotIgnored = 5 };
        using var connection = GetConnection();
        connection.Open();
        using var transaction = connection.BeginTransaction();
        var inserted = connection.BulkInsertAndSelect(new List<IdentityAndWriteInsertTests.IdentityAndWriteInsertTest> { item }, transaction).First();
        IsValidInsert(inserted, item);
    }

    private static void IsValidInsert(IdentityAndWriteInsertTests.IdentityAndWriteInsertTest inserted, IdentityAndWriteInsertTests.IdentityAndWriteInsertTest toBeInserted)
    {
        inserted.IdKey.Should().BePositive();
        inserted.Ignored.Should().NotBe(toBeInserted.Ignored);
        inserted.NotIgnored.Should().Be(toBeInserted.NotIgnored);
        inserted.Name.Should().Be(toBeInserted.Name);
    }
}
