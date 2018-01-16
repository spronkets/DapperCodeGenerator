using System;
using System.Collections.Generic;
using System.Linq;
using DapperCodeGenerator.Core.Models;

namespace DapperCodeGenerator.Core.Providers
{
    [Serializable]
    public abstract class Provider
    {
        public string ConnectionString { get; }
        
        protected Provider(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public List<Database> RefreshDatabases()
        {
            var databases = GetDatabases().ToList();
            return databases;
        }

        public Database SelectDatabase(List<Database> databases, string databaseName)
        {
            var database = databases.SingleOrDefault(d => d.DatabaseName == databaseName);

            if (database == null)
            {
                return null;
            }

            database.Tables = GetDatabaseTables(databaseName).ToList();

            foreach (var table in database.Tables)
            {
                table.Columns = GetDatabaseTableColumns(databaseName, table.TableName).ToList();
            }

            return database;
        }

        protected abstract IEnumerable<Database> GetDatabases();

        protected abstract IEnumerable<DatabaseTable> GetDatabaseTables(string databaseName);

        protected abstract IEnumerable<DatabaseTableColumn> GetDatabaseTableColumns(string databaseName, string tableName);

        protected abstract Type GetClrType(string dbTypeName, bool isNullable);
    }
}