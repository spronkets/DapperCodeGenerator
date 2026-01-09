using System;
using System.Collections.Generic;
using System.Linq;
using DapperCodeGenerator.Core.Models;

namespace DapperCodeGenerator.Core.Providers
{
    public abstract class Provider(string connectionString)
    {
        public string ConnectionString { get; } = connectionString;
        public string LastConnectionError { get; protected set; }

        public List<Database> RefreshDatabases(bool filterSystemObjects)
        {
            LastConnectionError = null;
            var databases = GetDatabases(filterSystemObjects).ToList();
            return databases;
        }

        public Database SelectDatabase(List<Database> databases, string databaseName, bool filterSystemObjects)
        {
            var database = databases.SingleOrDefault(d => d.DatabaseName == databaseName);

            if (database == null)
            {
                return null;
            }

            database.Tables = [.. GetDatabaseTables(databaseName, filterSystemObjects)];

            foreach (var table in database.Tables)
            {
                table.Columns = [.. GetDatabaseTableColumns(databaseName, table.TableName)];
            }

            return database;
        }

        protected abstract IEnumerable<Database> GetDatabases(bool filterSystemObjects);

        protected abstract IEnumerable<DatabaseTable> GetDatabaseTables(string databaseName, bool filterSystemObjects);

        protected abstract IEnumerable<DatabaseTableColumn> GetDatabaseTableColumns(string databaseName, string tableName);

        protected abstract Type GetClrType(string dbTypeName, bool isNullable);
    }
}