using System;
using System.Data;
using System.Data.SqlClient;

namespace Dapper.Bulk.Tests
{
    public class SqlServerTestSuite
    {
        private static string ConnectionString = "Data Source=192.168.1.105\\MKMSSQL;Initial Catalog=Words;User ID=MkWeddings;Password=Sup3rn@tural;";

        public IDbConnection GetConnection() => new SqlConnection(ConnectionString);

        static SqlServerTestSuite()
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                Func<string, string> dropTable = name => $"IF OBJECT_ID('{name}', 'U') IS NOT NULL DROP TABLE [{name}];";
                connection.Open();
                connection.Execute(
                    $@"{dropTable("IdentityAndComputedTests")}
                    CREATE TABLE IdentityAndComputedTests
                    (
	                    [IdKey] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	                    [Name] NVARCHAR(100) NULL,
	                    [CreateDate] DATETIME2 NOT NULL DEFAULT(GETDATE())
                    );");

                connection.Execute(
                    $@"{dropTable("NoIdentityAndComputedTests")}
                    CREATE TABLE NoIdentityAndComputedTests
                    (
	                    [IdKey] BIGINT NOT NULL PRIMARY KEY,
	                    [Name] NVARCHAR(100) NULL,
	                    [CreateDate] DATETIME2 NOT NULL DEFAULT(GETDATE())
                    );");

                connection.Execute(
                    $@"{dropTable("NoIdentityTests")}
                    CREATE TABLE NoIdentityTests(
	                    [ItemId] BIGINT NULL,
	                    [Name] NVARCHAR(100) NULL
                    );");                
            }
        }
    }
}
