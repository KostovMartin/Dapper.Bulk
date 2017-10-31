using Npgsql;
using System.Data;

namespace Dapper.Bulk.Tests
{
    public class PostgresTestSuite
    {
        private static string ConnectionString = "Server=localhost;Port=5432;Database=Tests;User Id=postgres;Password=Sup3rn@tural;";

        public IDbConnection GetConnection() => new NpgsqlConnection(ConnectionString);

        static PostgresTestSuite()
        {
            using (var connection = new NpgsqlConnection(ConnectionString))
            {
                string DropTable(string name) => $"DROP TABLE IF EXISTS {name};";
                connection.Execute($@"
                    {DropTable("tcat")}
                    CREATE TABLE tcat 
                    ( 
                        id serial not null, 
                        name character varying (200) not null
                    );");               
            }
        }
    }
}
