using FluentAssertions;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Dapper.Bulk.Tests;

public class EnumTests : SqlServerTestSuite
{
    private enum IntEnum
    {
        Odd = 1,
        Even = 2
    }

    private enum LongEnum : long
    {
        Min = long.MinValue,
        Max = long.MaxValue
    }

    private class EnumTest
    {
        public int Id { get; set; }

        public IntEnum IntEnum { get; set; }

        public LongEnum LongEnum { get; set; }
    }

    [Fact]
    public void InsertBulk()
    {
        var data = new List<EnumTest>();
        for (var i = 0; i < 10; i++)
        {
            data.Add(new EnumTest {
                Id = i,
                IntEnum = i % 2 != 0 ? IntEnum.Odd : IntEnum.Even,
                LongEnum = i % 2 != 0 ? LongEnum.Min : LongEnum.Max
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

    private static void IsValidInsert(EnumTest inserted, EnumTest toBeInserted)
    {
        inserted.Id.Should().BePositive();
        inserted.IntEnum.Should().Be(toBeInserted.IntEnum);
        inserted.LongEnum.Should().Be(toBeInserted.LongEnum);
    }
}
