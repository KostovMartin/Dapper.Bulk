using Dapper.Bulk.Tests.Attributes;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dapper.Bulk.Tests
{
    public class InsertWithSubclassTests : SqlServerTestSuite
    {
        class TestSublass
        {

        }
        private class IdentityAndComputedTest
        {
            [Key]
            public int IdKey { get; set; }

            public string Name { get; set; }

            [Computed]
            public DateTime? CreateDate { get; set; }

            public virtual TestSublass TestSublass { get; set; }

            [NotMapped]
            public int Ignored { get; set; }

        }

        [Fact]
        public void InsertBulk()
        {
            var data = new List<IdentityAndComputedTest>();
            for (var i = 0; i < 10; i++)
            {
                data.Add(new IdentityAndComputedTest { Name = Guid.NewGuid().ToString() });
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

        [Fact]
        public void InsertSingle()
        {
            var item = new IdentityAndComputedTest { Name = Guid.NewGuid().ToString() };
            using (var connection = this.GetConnection())
            {
                connection.Open();
                var inserted = connection.BulkInsertAndSelect(new List<IdentityAndComputedTest> { item }).First();
                IsValidInsert(inserted, item);
            }
        }

        [Fact]
        public async Task InsertSingleAsync()
        {
            var item = new IdentityAndComputedTest { Name = Guid.NewGuid().ToString() };
            using (var connection = this.GetConnection())
            {
                connection.Open();
                var inserted = (await connection.BulkInsertAndSelectAsync(new List<IdentityAndComputedTest> { item })).First();
                IsValidInsert(inserted, item);
            }
        }

        [Fact]
        public void InsertSingleTransaction()
        {
            var item = new IdentityAndComputedTest { Name = Guid.NewGuid().ToString() };
            using (var connection = this.GetConnection())
            {
                connection.Open();
                using (var transaction = connection.BeginTransaction())
                {
                    var inserted = connection.BulkInsertAndSelect(new List<IdentityAndComputedTest> { item }, transaction).First();
                    IsValidInsert(inserted, item);
                }
            }
        }

        private static void IsValidInsert(IdentityAndComputedTest inserted, IdentityAndComputedTest toBeInserted)
        {
            inserted.IdKey.Should().BePositive();
            inserted.Name.Should().Be(toBeInserted.Name);
            inserted.CreateDate.Should().BeAtLeast((DateTime.UtcNow - TimeSpan.FromDays(1)).TimeOfDay);
        }
    }
}
