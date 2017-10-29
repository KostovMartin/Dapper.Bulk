using FluentAssertions;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dapper.Bulk.Tests
{
    public class IdentityAndComputedInsertTests : SqlServerTestSuite
    {
        public class ComputedAttribute : Attribute
        {
        }

        public class IdentityAndComputedTest
        {
            [Key]
            public int IdKey { get; set; }

            public string Name { get; set; }

            [Computed]
            public DateTime? CreateDate { get; set; }
        }

        [Fact]
        public void InsertBulk()
        {
            var data = new List<IdentityAndComputedTest>();
            for (int i = 0; i < 10; i++)
            {
                data.Add(new IdentityAndComputedTest { Name = Guid.NewGuid().ToString() });
            }

            using (var connection = this.GetConnection())
            {
                connection.Open();
                var inserted = connection.BulkInsert(data).ToList();
                for (int i = 0; i < inserted.Count; i++)
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
                var inserted = connection.BulkInsert(new List<IdentityAndComputedTest> { item }).First();
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
                var inserted = (await connection.BulkInsertAsync(new List<IdentityAndComputedTest> { item })).First();
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
                    var inserted = connection.BulkInsert(new List<IdentityAndComputedTest> { item }, transaction).First();
                    IsValidInsert(inserted, item);
                }
            }
        }
        
        private void IsValidInsert(IdentityAndComputedTest inserted, IdentityAndComputedTest toBeInserted)
        {
            inserted.IdKey.Should().BePositive();
            inserted.Name.Should().Be(toBeInserted.Name);
            inserted.CreateDate.Should().BeAtLeast((DateTime.UtcNow - TimeSpan.FromDays(1)).TimeOfDay);
        }
    }
}
