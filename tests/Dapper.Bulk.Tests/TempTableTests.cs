using Dapper.Bulk.Tests.Attributes;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dapper.Bulk.Tests
{

    public class TempTableTests : SqlServerTestSuite
    {

        public enum AnEnum
        {
            Val1,
            Val2
        }
        private class TempTableMap
        {
            [Key]
            public int IntField { get; set; }
            public int LongField { get; set; }
            public byte ByteField { get; set; }
            public bool BoolField { get; set; }
            public string StringField { get; set; }
            public DateTime DateTimeField { get; set; }
            public AnEnum EnumField {get;set; }
            public Guid GuidField { get; set; }
            public Double DoubleField { get; set; }
            public float FloatField { get; set; }
            public sbyte SByteField { get; set; }
            public Char CharleField { get; set; }
            public uint UintField { get; set; }
            public ulong ULongField { get; set; }
            public short ShortField { get; set; }

        }

        [Fact]
        public void InsertBulkIntoTempTable()
        {
            var rnd=new Random();

            var data = new List<TempTableMap>();
            for (var i = 0; i < 10000; i++)
            {
                data.Add(new TempTableMap {
                    BoolField= rnd.Next(2)==1,
                    DateTimeField=DateTime.Now,
                    ByteField=(byte)(rnd.Next(256)-1),
                    EnumField=Enum.GetValues<AnEnum>()[rnd.Next(2)],
                    GuidField=Guid.NewGuid(),
                    IntField=rnd.Next(),
                    LongField=rnd.Next(),
                    StringField=Guid.NewGuid().ToString(),
                    CharleField=(Char)(rnd.Next(20,256)),
                    DoubleField=rnd.NextDouble(),
                    FloatField=(float)rnd.NextDouble(),
                    SByteField= (sbyte)(rnd.Next(256) - 128),
                    ShortField = (short)(rnd.Next(32768) - 128),
                    UintField = (uint)rnd.Next(1 << 30),
                    ULongField= (ulong)rnd.Next()
                });
            }

            using (var connection = this.GetConnection())
            {
                connection.Open();
                connection.BulkInsertIntoTempTable(data,"#temp1");
                var tempTable=connection.Query<TempTableMap>("select * from #temp1").ToList();
                for(var i=0;i<tempTable.Count();i++)
                {
                    data[i].IntField.Should().Be(tempTable[i].IntField);
                    data[i].LongField.Should().Be(tempTable[i].LongField);
                    data[i].StringField.Should().Be(tempTable[i].StringField);
                    data[i].BoolField.Should().Be(tempTable[i].BoolField);
                    data[i].ByteField.Should().Be(tempTable[i].ByteField);
                    data[i].DateTimeField.Should().Be(tempTable[i].DateTimeField);
                    data[i].EnumField.Should().Be(tempTable[i].EnumField);
                    data[i].StringField.Should().Be(tempTable[i].StringField);
                    data[i].CharleField.Should().Be(tempTable[i].CharleField);
                    data[i].DoubleField.Should().Be(tempTable[i].DoubleField);
                    data[i].FloatField.Should().Be(tempTable[i].FloatField);
                    data[i].SByteField.Should().Be(tempTable[i].SByteField);
                    data[i].ShortField.Should().Be(tempTable[i].ShortField);
                    data[i].UintField.Should().Be(tempTable[i].UintField);
                    data[i].ULongField.Should().Be(tempTable[i].ULongField);
                    

                }

            }
        }
        
       
    }
}
