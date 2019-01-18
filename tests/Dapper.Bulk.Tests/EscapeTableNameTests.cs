using FluentAssertions;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Xunit;

namespace Dapper.Bulk.Tests
{
    public class EscapeTableNameTests : SqlServerTestSuite
    {
        [Table("10_Escapes")]
        private class Escape
        {
            public int Id { get; set; }

            [Column("10_Name")]
            public string Name { get; set; }
        }

        [Fact]
        public void EscapesTest()
        {
            var data = new List<Escape>();
            for (var i = 0; i < 10; i++)
            {
                data.Add(new Escape {
                    Id = i,
                    Name = i.ToString()
                });
            }

            using (var connection = this.GetConnection())
            {
                connection.Open();
                var inserted = connection.BulkInsertAndSelect(data).ToList();
                for (var i = 0; i < data.Count; i++)
                {
                    IsValidInsert(inserted[i], data[i]);
                }
            }
        }

        private static void IsValidInsert(Escape inserted, Escape toBeInserted)
        {
            inserted.Id.Should().BePositive();
            inserted.Name.Should().Be(toBeInserted.Name);
        }
    }
}
