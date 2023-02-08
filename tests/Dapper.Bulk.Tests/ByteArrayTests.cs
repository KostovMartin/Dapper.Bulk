using FluentAssertions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;

namespace Dapper.Bulk.Tests;

public class ByteArrayTests : SqlServerTestSuite
{
    private class ByteArray
    {
        public int Id { get; set; }

        public byte[] TestArray { get; set; }
    }

    [Fact]
    public void InsertBulk()
    {
        var data = new List<ByteArray>();
        for (var i = 0; i < 10; i++)
        {
            data.Add(new ByteArray
            {
                Id = i,
                TestArray = Encoding.ASCII.GetBytes(Path.GetRandomFileName()),
            });
        }

        using var connection = GetConnection();
        connection.Open();
        var inserted = connection.BulkInsertAndSelect(data).ToList();
        for (var i = 0; i < data.Count; i++)
        {
            IsValidInsert(inserted[i], data[i]);
        }
    }

    private static void IsValidInsert(ByteArray inserted, ByteArray toBeInserted)
    {
        inserted.Id.Should().BePositive();
        inserted.TestArray.Should().Match(x => x.Select(y => toBeInserted.TestArray.Contains(y)).Any());
    }
}