using System;
using FluentAssertions;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Dapper.Bulk.Tests.Attributes;
using Xunit;

namespace Dapper.Bulk.Tests
{
    public class PgInsertTests : PostgresTestSuite
    {
        private class IdentityAndComputedTest
        {
            [Key]
            public int Id { get; set; }

            public string Name { get; set; }

            [Computed]
            public DateTime? CreateDate { get; set; }
        }

        //[Fact]
        public void InsertBulk()
        {
            var data = new List<IdentityAndComputedTest>();
            for (var i = 0; i < 100000; i++)
            {
                data.Add(new IdentityAndComputedTest { Name = Guid.NewGuid().ToString().Take(10).ToString() });
            }

            using (var conn = GetConnection())
            {
                conn.Open();

                //conn.Execute("COPY data (name) FROM STDIN", data);
                //conn.sa

                conn.Execute("insert into tcat(name) values(:name) ", data);

                var r = conn.Query<IdentityAndComputedTest>("select * from tcat where id=any(:catids)", new { catids = new[] { 1, 3, 5 } });
                Assert.Equal(3, r.Count());
                Assert.Equal(1, r.Count(c => c.Id == 1));
                Assert.Equal(1, r.Count(c => c.Id == 3));
                Assert.Equal(1, r.Count(c => c.Id == 5));
            }
        }
    }
}
