using System.Data.SqlClient;

namespace Dapper.Bulk.Tests
{
    public class SqlServerTestSuite
    {
        private static string ConnectionString = "Server=(localdb)\\mssqllocaldb;Database=DapperBulkTest;Trusted_Connection=True;MultipleActiveResultSets=true;";

        public SqlConnection GetConnection() => new SqlConnection(ConnectionString);

        static SqlServerTestSuite()
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                string DropTable(string name) => $"IF OBJECT_ID('{DapperBulk.FormatTableName(name)}', 'U') IS NOT NULL DROP TABLE {DapperBulk.FormatTableName(name)};";
                string DropSchema(string name) => $"IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{name}') DROP SCHEMA {name}";
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

                connection.Execute(
                    $@"{DropTable("ByteArrays")}
                    CREATE TABLE ByteArrays(
	                    [Id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	                    [TestArray] varbinary(100) NOT NULL
                    );");
                
                connection.Execute(
                    $@"{DropTable("IdentityInsertEnabledTests")}
                    CREATE TABLE IdentityInsertEnabledTests
                    (
	                    [IdKey] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	                    [Name] NVARCHAR(100) NULL
                    );");
                
                connection.Execute(
                  $@"{DropTable("IdentityAndNotMappedTests")}
                    CREATE TABLE IdentityAndNotMappedTests
                    (
	                    [IdKey] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	                    [Name] NVARCHAR(100) NULL
                    );");

                connection.Execute(
                  $@"{DropTable("CustomColumnNames")}
                    CREATE TABLE CustomColumnNames
                    (
	                    [Id_Key] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	                    [Name_1] NVARCHAR(100) NULL,
                        [Int_Col] INT NOT NULL,
	                    [Long_Col] BIGINT NOT NULL
                    );");

                connection.Execute(
                    $@"{DropTable("10_Escapes")}
                    CREATE TABLE [10_Escapes](
	                    [Id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	                    [10_Name] NVARCHAR(100) NULL
                    );");

                var schemaName = "test";

                connection.Execute(
                $@"{DropTable($@"{schemaName}.10_Escapes")}");

                connection.Execute(
                    $@"{DropSchema(schemaName)}");

                connection.Execute(
                    $@"CREATE SCHEMA {schemaName}");

                connection.Execute(
                    $@"CREATE TABLE [{schemaName}].[10_Escapes](
	                    [Id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	                    [10_Name] NVARCHAR(100) NULL
                    );");

                connection.Execute(
                    $@"{DropTable("PE_TranslationPhrase")}
                    CREATE TABLE [PE_TranslationPhrase](
	                    [TranslationId] INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	                    [CultureName] NVARCHAR(100) NOT NULL,
	                    [Phrase] NVARCHAR(100) NOT NULL,
	                    [PhraseHash] uniqueidentifier NULL,
	                    [RowAddedDateTime] DATETIME2 NOT NULL DEFAULT(GETDATE())
                    );");

                connection.Execute(
                    $@"{DropTable("IdentityAndWriteInsertTests")}
                    CREATE TABLE IdentityAndWriteInsertTests
                    (
	                    [IdKey] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
	                    [Name] NVARCHAR(100) NULL,
	                    [NotIgnored] INT NULL
                    );");
            }
        }
    }
}
