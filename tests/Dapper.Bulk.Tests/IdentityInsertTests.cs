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
    public class IdentityInsertTests
    {
        private static readonly string _connectionString = "";

        [Table("Table_1")]
        public class Node
        {
            public int Id { get; set; }

            public string Name1 { get; set; }

            public string Name2 { get; set; }
        }

        [Fact]
        public void InsertBulk()
        {
            var data = new List<Node>();
            for (int i = 0; i < 10; i++)
            {
                data.Add(new Node { Name1 = Guid.NewGuid().ToString(), Name2 = Guid.NewGuid().ToString() });
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
            var item = new Node { Name1 = Guid.NewGuid().ToString(), Name2 = Guid.NewGuid().ToString() };            
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
            var item = new Node { Name1 = Guid.NewGuid().ToString(), Name2 = Guid.NewGuid().ToString() };
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
            var item = new Node { Name1 = Guid.NewGuid().ToString(), Name2 = Guid.NewGuid().ToString() };
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
            toBeInserted.Id.Should().Be(0);
            inserted.Id.Should().BePositive();
            inserted.Name1.Should().Be(toBeInserted.Name1);
            inserted.Name2.Should().Be(toBeInserted.Name2);
        }
    }
}
