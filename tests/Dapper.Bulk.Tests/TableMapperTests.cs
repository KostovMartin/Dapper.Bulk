using FluentAssertions;
using Xunit;
using System.ComponentModel.DataAnnotations.Schema;

namespace Dapper.Bulk.Tests;

public class TableMapperTests
{
    private class Node
    {
    }

    [Table("Node2Table", Schema = "TestSchema")]
    private class Node2
    {
    }

    [Fact]
    public void DefaultConvention_Should_Match()
    {
        var name = TableMapper.GetTableName(typeof(Node));

        name.Should().Be("Nodes");
    }

    [Fact]
    public void DefaultConvention_Should_Not_Match()
    {
        var name = TableMapper.GetTableName(typeof(Node));

        name.Should().NotBe("Node");
    }


    [Fact]
    public void ReadSchema()
    {
        var name = TableMapper.GetTableName(typeof(Node2));

        name.Should().Be("TestSchema.Node2Table");
    }

    //[Fact]
    //public void tbl_s_Convention_Should_Match()
    //{
    //    TableMapper.SetupConvention("tbl", "s");
    //    var name = TableMapper.GetTableName(typeof(Node));

    //    name.Should().Be("tblNodes");
    //}

    //[Fact]
    //public void tbl_s_Convention_Should_Not_Match()
    //{
    //    TableMapper.SetupConvention("tbl", "s");
    //    var name = TableMapper.GetTableName(typeof(Node));

    //    name.Should().NotBe("Nodes");
    //}
}
