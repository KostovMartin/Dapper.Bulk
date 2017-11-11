using FluentAssertions;
using Xunit;

namespace Dapper.Bulk.Tests
{
    public class TableMapperTests
    {
        private class Node
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
}
