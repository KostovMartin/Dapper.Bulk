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
    public class CustomColumnNameTests: SqlServerTestSuite
    {
        private class CustomColumnName
        {
            [Key, Column("Id_Key")]
            public int IdKey { get; set; }

            [Column("Name_1")]
            public string Name { get; set; }

            [Column("Int_Col")]
            public int IntCol { get; set; }

            [Column("Long_Col")]
            public long LongCol { get; set; }

            [NotMapped]
            public int Ignored { get; set; }
        }

        [Fact]
        public void InsertBulk()
        {
            var data = new List<CustomColumnName>();
            for (var i = 0; i < 10; i++)
            {
                data.Add(new CustomColumnName { Name = Guid.NewGuid().ToString() , LongCol = i * 1000, IntCol = i});
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
            var item = new CustomColumnName { Name = Guid.NewGuid().ToString(), LongCol = 1000, IntCol = 1 };
            using (var connection = this.GetConnection())
            {
                connection.Open();
                var inserted = connection.BulkInsertAndSelect(new List<CustomColumnName> { item }).First();
                IsValidInsert(inserted, item);
            }
        }

        [Fact]
        public async Task InsertSingleAsync()
        {
            var item = new CustomColumnName { Name = Guid.NewGuid().ToString(), LongCol = 1000, IntCol = 1 };
            using (var connection = this.GetConnection())
            {
                connection.Open();
                var inserted = (await connection.BulkInsertAndSelectAsync(new List<CustomColumnName> { item })).First();
                IsValidInsert(inserted, item);
            }
        }

        [Fact]
        public void InsertSingleTransaction()
        {
            var item = new CustomColumnName { Name = Guid.NewGuid().ToString(), LongCol = 1000, IntCol = 1 };
            using (var connection = this.GetConnection())
            {
                connection.Open();
                using (var transaction = connection.BeginTransaction())
                {
                    var inserted = connection.BulkInsertAndSelect(new List<CustomColumnName> { item }, transaction).First();
                    IsValidInsert(inserted, item);
                }
            }
        }

        private static void IsValidInsert(CustomColumnName inserted, CustomColumnName toBeInserted)
        {
            inserted.IdKey.Should().BePositive();
            inserted.Name.Should().Be(toBeInserted.Name);
            
        }
    }
}
