using Dapper;
using DapperCodeGenerator.Core.Enumerations;
using DapperCodeGenerator.Core.Models;
using Microsoft.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace DapperCodeGenerator.Core.Providers
{
    public class PostgresProvider(string connectionString) : Provider(connectionString)
    {
        private readonly string[] _systemDatabases = ["system", "template0", "template1"];
        private readonly string[] _systemTables = ["VersionInfo", "pg_", "sql_"];

        private readonly NpgsqlConnectionStringBuilder _connectionStringBuilder = new(connectionString) { Database = "" };

        protected override IEnumerable<Database> GetDatabases()
        {
            DataTable databases = null;
            try
            {
                using var db = new NpgsqlConnection(_connectionStringBuilder.ToString());
                db.Open();
                databases = db.GetSchema(SqlClientMetaDataCollectionNames.Databases);
                db.Close();
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message, exc);
            }

            if (databases != null)
            {
                foreach (DataRow databaseRow in databases.Rows)
                {
                    var databaseName = databaseRow.ItemArray[0].ToString();
                    if (!_systemDatabases.Any(d => d.Equals(databaseName, StringComparison.InvariantCultureIgnoreCase)))
                    {
                        var database = new Database
                        {
                            ConnectionType = DbConnectionTypes.Postgres,
                            DatabaseName = databaseName
                        };
                        yield return database;
                    }
                }
            }
        }

        protected override IEnumerable<DatabaseTable> GetDatabaseTables(string databaseName)
        {
            DataTable selectedDatabaseTables = null;
            try
            {
                using var db = new NpgsqlConnection($"{_connectionStringBuilder};Database={databaseName};");
                db.Open();
                selectedDatabaseTables = db.GetSchema(SqlClientMetaDataCollectionNames.Tables);
                db.Close();
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message, exc);
            }

            if (selectedDatabaseTables != null)
            {
                foreach (DataRow tableRow in selectedDatabaseTables.Rows)
                {
                    var tableName = tableRow.ItemArray[2].ToString();
                    if (!_systemTables.Any(t => tableName.StartsWith(t)))
                    {
                        var table = new DatabaseTable
                        {
                            ConnectionType = DbConnectionTypes.Postgres,
                            DatabaseName = databaseName,
                            TableName = tableName
                        };
                        yield return table;
                    }
                }
            }
        }

        protected override IEnumerable<DatabaseTableColumn> GetDatabaseTableColumns(string databaseName, string tableName)
        {
            DataTable selectedDatabaseTableColumns = null;
            List<dynamic> selectedDatabaseTableConstraints = null;

            try
            {
                using var db = new NpgsqlConnection($"{_connectionStringBuilder};Database={databaseName};");
                db.Open();

                var columnRestrictions = new string[3];
                columnRestrictions[0] = databaseName;
                columnRestrictions[2] = tableName;

                selectedDatabaseTableColumns = db.GetSchema(SqlClientMetaDataCollectionNames.Columns, columnRestrictions);

                selectedDatabaseTableConstraints = db.Query(
                    "SELECT a.attname, c.conname, c.contype FROM pg_constraint c JOIN pg_attribute a ON a.attnum = ANY(c.conkey) WHERE c.conrelid = @table::regclass AND (c.contype = 'p' OR c.contype = 'u')",
                    new { table = tableName }).ToList();

                db.Close();
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message, exc);
            }

            if (selectedDatabaseTableColumns != null)
            {
                foreach (DataRow columnRow in selectedDatabaseTableColumns.Rows)
                {
                    var columnName = columnRow.ItemArray[3].ToString();
                    var isNullable = columnRow.ItemArray[6].ToString() == "YES";
                    var dataType = columnRow.ItemArray[7].ToString();
                    var type = GetClrType(dataType, isNullable);
                    var maxLengthStr = columnRow.ItemArray[8].ToString();
                    int.TryParse(maxLengthStr, out var maxLength);

                    var column = new DatabaseTableColumn
                    {
                        ConnectionType = DbConnectionTypes.Postgres,
                        DatabaseName = databaseName,
                        TableName = tableName,
                        ColumnName = columnName,
                        DataType = dataType,
                        Type = type,
                        TypeNamespace = type.Namespace,
                        MaxLength = maxLength
                    };

                    if (selectedDatabaseTableConstraints != null)
                    {
                        foreach (var constraint in selectedDatabaseTableConstraints)
                        {
                            if (constraint.attname == columnName)
                            {
                                if (constraint.contype == 'p')
                                {
                                    column.PrimaryKeys.Add(constraint.conname);
                                }
                                else if (constraint.contype == 'u')
                                {
                                    column.UniqueKeys.Add(constraint.conname);
                                }
                            }
                        }
                    }

                    yield return column;
                }
            }
        }

        protected override Type GetClrType(string dbTypeName, bool isNullable)
        {
            return dbTypeName switch
            {
                "int8" => isNullable ? typeof(long?) : typeof(long),
                "bytea" => typeof(byte[]),
                "bool" => isNullable ? typeof(bool?) : typeof(bool),
                "text" or "varchar" or "bpchar" or "citext" or "json" or "jsonb" or "xml" or "name" => typeof(string),
                "point" => typeof(NpgsqlPoint),
                "lseg" => typeof(NpgsqlLSeg),
                "path" => typeof(NpgsqlPath),
                "polygon" => typeof(NpgsqlPolygon),
                "line" => typeof(NpgsqlLine),
                "circle" => typeof(NpgsqlCircle),
                "box" => typeof(NpgsqlBox),
                "date" or "timestamp" or "timestamptz" => isNullable ? typeof(DateTime?) : typeof(DateTime),
                "numeric" or "money" => isNullable ? typeof(decimal?) : typeof(decimal),
                "float8" => isNullable ? typeof(double?) : typeof(double),
                "int4" => isNullable ? typeof(int?) : typeof(int),
                "float4" => isNullable ? typeof(float?) : typeof(float),
                "uuid" => isNullable ? typeof(Guid?) : typeof(Guid),
                "int2" => isNullable ? typeof(short?) : typeof(short),
                "tinyint" => isNullable ? typeof(byte?) : typeof(byte),
                "structured" => typeof(DataTable),
                "timetz" => isNullable ? typeof(DateTimeOffset?) : typeof(DateTimeOffset),
                _ => typeof(object)
            };
        }
    }
}
