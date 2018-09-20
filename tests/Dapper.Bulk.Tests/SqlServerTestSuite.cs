using System.Data.SqlClient;

namespace Dapper.Bulk.Tests
{
    public class SqlServerTestSuite
    {
        private static string ConnectionString = "Server=localhost;DataBase=Drp;Persist Security Info=True;User ID=test ;Password=Qwerty12345";

        public SqlConnection GetConnection() => new SqlConnection(ConnectionString);

        static SqlServerTestSuite()
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                string DropTable(string name) => $"IF OBJECT_ID('{name}', 'U') IS NOT NULL DROP TABLE [{name}];";
                connection.Open();
                connection.Execute(
                    $@"{DropTable("IdentityAndComputedTests")}
                    CREATE TABLE IdentityAndComputedTests
                    (
	                    [IdKey] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	                    [Name] NVARCHAR(100) NULL,
	                    [CreateDate] DATETIME2 NOT NULL DEFAULT(GETDATE())
                    );");
                
                connection.Execute(
                    $@"{DropTable("NoIdentityTests")}
                    CREATE TABLE NoIdentityTests(
	                    [ItemId] BIGINT NULL,
	                    [Name] NVARCHAR(100) NULL
                    );");

                connection.Execute(
                    $@"{DropTable("EnumTests")}
                    CREATE TABLE EnumTests(
	                    [Id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	                    [IntEnum] INT NOT NULL,
	                    [LongEnum] BIGINT NOT NULL
                    );");
            }
        }
    }
}
