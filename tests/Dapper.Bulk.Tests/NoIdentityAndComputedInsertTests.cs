using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dapper.Bulk.Tests
{
    public class NoIdentityAndComputedInsertTests : SqlServerTestSuite
    {
        private class ComputedAttribute : Attribute
        {
        }

        private class NoIdentityAndComputedTest
        {
            public int IdKey { get; set; }

            public string Name { get; set; }

            [Computed]
            public DateTime? CreateDate { get; set; }
        }

        [Fact]
        public void InsertBulk()
        {
            var data = new List<NoIdentityAndComputedTest>();
            for (int i = 1; i < 11; i++)
            {
                data.Add(new NoIdentityAndComputedTest { IdKey = i, Name = Guid.NewGuid().ToString() });
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
            var item = new NoIdentityAndComputedTest { IdKey = 100, Name = Guid.NewGuid().ToString() };
            using (var connection = this.GetConnection())
            {
                connection.Open();
                var inserted = connection.BulkInsert(new List<NoIdentityAndComputedTest> { item }).First();
                IsValidInsert(inserted, item);
            }
        }

        [Fact]
        public async Task InsertSingleAsync()
        {
            var item = new NoIdentityAndComputedTest { IdKey = 101, Name = Guid.NewGuid().ToString() };
            using (var connection = this.GetConnection())
            {
                connection.Open();
                var inserted = (await connection.BulkInsertAsync(new List<NoIdentityAndComputedTest> { item })).First();
                IsValidInsert(inserted, item);
            }
        }

        [Fact]
        public void InsertSingleTransaction()
        {
            var item = new NoIdentityAndComputedTest { IdKey = 102, Name = Guid.NewGuid().ToString() };
            using (var connection = this.GetConnection())
            {
                connection.Open();
                using (var transaction = connection.BeginTransaction())
                {
                    var inserted = connection.BulkInsert(new List<NoIdentityAndComputedTest> { item }, transaction).First();
                    IsValidInsert(inserted, item);
                }
            }
        }
        
        private void IsValidInsert(NoIdentityAndComputedTest inserted, NoIdentityAndComputedTest toBeInserted)
        {
            inserted.IdKey.Should().BePositive();
            inserted.Name.Should().Be(toBeInserted.Name);
            inserted.CreateDate.Should().BeAtLeast((DateTime.UtcNow - TimeSpan.FromDays(1)).TimeOfDay);
        }
    }
}
