using FluentAssertions;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dapper.Bulk.Tests
{
    public class NoIdentityInsertTests
    {
        private static readonly string _connectionString = "Data Source=192.168.1.105\\MKMSSQL;Initial Catalog=Words;User ID=MkWeddings;Password=Sup3rn@tural;";

        [Table("Table_2")]
        public class Node
        {
            public int ItemId { get; set; }
            
            public string Name { get; set; }
        }

        [Fact]
        public void InsertBulk()
        {
            var data = new List<Node>();
            for (int i = 0; i < 10; i++)
            {
                data.Add(new Node { ItemId = i, Name = Guid.NewGuid().ToString() });
            }
            
            using (SqlConnection connection = new SqlConnection(_connectionString))
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
            var item = new Node { ItemId = 1, Name = Guid.NewGuid().ToString() };
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                var inserted = connection.BulkInsert(new List<Node> { item }).First();
                IsValidInsert(inserted, item);
            }
        }

        [Fact]
        public async Task InsertSingleAsync()
        {
            var item = new Node { ItemId = 1, Name = Guid.NewGuid().ToString() };
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                var inserted = (await connection.BulkInsertAsync(new List<Node> { item })).First();
                IsValidInsert(inserted, item);
            }
        }

        [Fact]
        public void InsertSingleTransaction()
        {
            var item = new Node { ItemId = 1, Name = Guid.NewGuid().ToString() };
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                using (var transaction = connection.BeginTransaction())
                {
                    var inserted = connection.BulkInsert(new List<Node> { item }, transaction).First();
                    IsValidInsert(inserted, item);
                }
            }
        }

        private void IsValidInsert(Node inserted, Node toBeInserted)
        {
            inserted.ItemId.Should().Be(toBeInserted.ItemId);
            inserted.Name.Should().Be(toBeInserted.Name);
        }
    }
}
