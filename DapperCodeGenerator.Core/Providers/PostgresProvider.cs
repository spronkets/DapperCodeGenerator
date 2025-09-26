using Dapper;
using DapperCodeGenerator.Core.Enumerations;
using DapperCodeGenerator.Core.Models;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace DapperCodeGenerator.Core.Providers
{
    internal class PostgresColumnData
    {
        public string ColumnName { get; set; } = string.Empty;
        public string IsNullable { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public int? CharacterMaximumLength { get; set; }
        public int? NumericPrecision { get; set; }
        public int? NumericScale { get; set; }
        public string ColumnDefault { get; set; }
        public int OrdinalPosition { get; set; }
        public bool? IsIdentity { get; set; }
        public bool? IsGenerated { get; set; }
    }

    internal class PostgresConstraint
    {
        public string ColumnName { get; set; } = string.Empty;
        public string ConstraintName { get; set; } = string.Empty;
        public string ConstraintType { get; set; } = string.Empty;
    }

    public class PostgresProvider(string connectionString) : Provider(connectionString)
    {
        private readonly string[] _systemDatabases = ["system", "template0", "template1"];
        private readonly string[] _systemTables = ["VersionInfo", "pg_", "sql_", "information_schema"];

        private readonly NpgsqlConnectionStringBuilder _connectionStringBuilder = new(connectionString) { Database = "" };

        protected override IEnumerable<Database> GetDatabases()
        {
            try
            {
                using var db = new NpgsqlConnection(_connectionStringBuilder.ToString());

                const string databasesQuery = @"
                    SELECT datname as DatabaseName
                    FROM pg_database
                    WHERE datistemplate = false";

                var databases = db.Query<Database>(databasesQuery)
                    .Where(database => !_systemDatabases.Any(d => d.Equals(database.DatabaseName, StringComparison.InvariantCultureIgnoreCase)));
                Console.WriteLine($"PostgreSQL databases query returned {databases.Count()} filtered databases");
                return databases.Select(database =>
                {
                    database.ConnectionType = DbConnectionTypes.Postgres;
                    return database;
                });
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message, exc);
                return [];
            }
        }

        protected override IEnumerable<DatabaseTable> GetDatabaseTables(string databaseName)
        {
            try
            {
                using var db = new NpgsqlConnection($"{_connectionStringBuilder};Database={databaseName};");

                const string tablesQuery = @"
                    SELECT table_name as TableName
                    FROM information_schema.tables
                    WHERE table_type = 'BASE TABLE'
                        AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                        AND table_schema !~ '^pg_temp_'
                        AND table_schema !~ '^pg_toast_temp_'
                        AND table_schema = 'public'";

                var tables = db.Query<DatabaseTable>(tablesQuery)
                    .Where(table => !_systemTables.Any(t => table.TableName.StartsWith(t)));
                Console.WriteLine($"PostgreSQL tables query returned {tables.Count()} filtered tables for database {databaseName}");
                return tables.Select(table =>
                {
                    table.ConnectionType = DbConnectionTypes.Postgres;
                    table.DatabaseName = databaseName;
                    return table;
                });
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message, exc);
                return [];
            }
        }

        protected override IEnumerable<DatabaseTableColumn> GetDatabaseTableColumns(string databaseName, string tableName)
        {
            var columns = new List<DatabaseTableColumn>();

            try
            {
                using var db = new NpgsqlConnection($"{_connectionStringBuilder};Database={databaseName};");

                const string columnsQuery = @"
                    SELECT
                        column_name as ColumnName,
                        is_nullable as IsNullable,
                        data_type as DataType,
                        character_maximum_length as CharacterMaximumLength,
                        numeric_precision as NumericPrecision,
                        numeric_scale as NumericScale,
                        column_default as ColumnDefault,
                        ordinal_position as OrdinalPosition,
                        CASE WHEN is_identity = 'YES' THEN true
                             WHEN is_identity = 'NO' THEN false
                             ELSE null END as IsIdentity,
                        CASE WHEN is_generated = 'ALWAYS' THEN true
                             WHEN is_generated = 'NEVER' THEN false
                             ELSE null END as IsGenerated
                    FROM information_schema.columns
                    WHERE table_name = @tableName
                        AND table_schema = 'public'
                    ORDER BY ordinal_position";

                var postgresColumns = db.Query<PostgresColumnData>(columnsQuery, new { tableName });
                Console.WriteLine($"PostgreSQL columns query returned {postgresColumns.Count()} columns for table {tableName} in database {databaseName}");

                const string constraintsQuery = @"
                    SELECT
                        a.attname as ColumnName,
                        c.conname as ConstraintName,
                        CASE c.contype
                            WHEN 'p' THEN 'PRIMARY KEY'
                            WHEN 'u' THEN 'UNIQUE'
                            WHEN 'f' THEN 'FOREIGN KEY'
                        END as ConstraintType
                    FROM pg_constraint c
                    JOIN pg_attribute a ON a.attnum = ANY(c.conkey)
                    WHERE c.conrelid = @tableName::regclass
                        AND c.contype IN ('p', 'u', 'f')";

                var postgresConstraints = db.Query<PostgresConstraint>(constraintsQuery, new { tableName });
                Console.WriteLine($"PostgreSQL constraints query returned {postgresConstraints.Count()} constraints for table {tableName} in database {databaseName}");

                foreach (var postgresColumn in postgresColumns)
                {
                    var isNullable = postgresColumn.IsNullable == "YES";
                    var type = GetClrType(postgresColumn.DataType, isNullable);
                    var maxLength = postgresColumn.CharacterMaximumLength ?? 0;

                    var column = new DatabaseTableColumn
                    {
                        ConnectionType = DbConnectionTypes.Postgres,
                        DatabaseName = databaseName,
                        TableName = tableName,
                        ColumnName = postgresColumn.ColumnName,
                        DataType = postgresColumn.DataType,
                        Type = type,
                        TypeNamespace = type.Namespace,
                        MaxLength = maxLength,
                        NumericPrecision = postgresColumn.NumericPrecision,
                        NumericScale = postgresColumn.NumericScale,
                        DefaultValue = postgresColumn.ColumnDefault,
                        OrdinalPosition = postgresColumn.OrdinalPosition,
                        IsNullable = isNullable,
                        IsAutoIncrement = DetermineIsAutoIncrement(postgresColumn.IsIdentity, postgresColumn.ColumnDefault),
                        IsComputed = false,
                        IsGenerated = postgresColumn.IsGenerated
                    };

                    foreach (var constraint in postgresConstraints.Where(c => c.ColumnName == postgresColumn.ColumnName))
                    {
                        switch (constraint.ConstraintType)
                        {
                            case "PRIMARY KEY":
                                column.PrimaryKeys.Add(constraint.ConstraintName);
                                break;
                            case "UNIQUE":
                                column.UniqueKeys.Add(constraint.ConstraintName);
                                break;
                            case "FOREIGN KEY":
                                column.ForeignKeys.Add(constraint.ConstraintName);
                                break;
                        }
                    }

                    columns.Add(column);
                }
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message, exc);
            }

            return columns;
        }

        private static bool? DetermineIsAutoIncrement(bool? isIdentity, string columnDefault)
        {
            if (isIdentity == true)
            {
                return true;
            }

            if (columnDefault?.Contains("nextval") == true)
            {
                return true;
            }

            if (isIdentity == false && columnDefault?.Contains("nextval") != true)
            {
                return false;
            }

            return null;
        }

        protected override Type GetClrType(string dbTypeName, bool isNullable)
        {
            var lowerTypeName = dbTypeName.ToLowerInvariant();

            return lowerTypeName switch
            {
                "boolean" or "bool" => isNullable ? typeof(bool?) : typeof(bool),
                "smallint" or "int2" => isNullable ? typeof(short?) : typeof(short),
                "smallserial" => isNullable ? typeof(short?) : typeof(short),
                "integer" or "int4" => isNullable ? typeof(int?) : typeof(int),
                "serial" => isNullable ? typeof(int?) : typeof(int),
                "bigint" or "int8" => isNullable ? typeof(long?) : typeof(long),
                "bigserial" => isNullable ? typeof(long?) : typeof(long),
                "real" or "float4" => isNullable ? typeof(float?) : typeof(float),
                "double precision" or "float8" => isNullable ? typeof(double?) : typeof(double),
                "decimal" or "numeric" => isNullable ? typeof(decimal?) : typeof(decimal),
                "money" => isNullable ? typeof(decimal?) : typeof(decimal),
                "oid" or "regclass" or "regoper" or "regoperator" or "regproc" or "regprocedure" or "regtype" => isNullable ? typeof(uint?) : typeof(uint),

                "character" or "char" => typeof(string),
                "character varying" or "varchar" => typeof(string),
                "name" or "text" => typeof(string),
                "cidr" => typeof(string),
                "citext" => typeof(string),
                "json" or "jsonb" => typeof(string),
                "lquery" or "ltree" or "ltxtquery" => typeof(string),
                "tsquery" or "tsvector" => typeof(string),
                "xml" => typeof(string),
                "daterange" or "int4range" or "int8range" or "numrange" or "tsrange" or "tstzrange" => typeof(string),

                "date" => isNullable ? typeof(DateOnly?) : typeof(DateOnly),
                "time" or "time without time zone" => isNullable ? typeof(TimeOnly?) : typeof(TimeOnly),
                "timestamp" or "timestamp without time zone" => isNullable ? typeof(DateTime?) : typeof(DateTime),
                "timetz" or "time with time zone" => isNullable ? typeof(DateTimeOffset?) : typeof(DateTimeOffset),
                "timestamptz" or "timestamp with time zone" => isNullable ? typeof(DateTimeOffset?) : typeof(DateTimeOffset),
                "interval" => isNullable ? typeof(TimeSpan?) : typeof(TimeSpan),

                "uuid" => isNullable ? typeof(Guid?) : typeof(Guid),

                "bytea" => typeof(byte[]),

                "int2vector" => typeof(short[]),
                "oidvector" => typeof(uint[]),

                "bit" or "bit varying" or "varbit" => typeof(BitArray),
                _ when lowerTypeName.EndsWith("[]") => typeof(Array),
                "hstore" => typeof(Dictionary<string, string>),

                "inet" => typeof(System.Net.IPAddress),
                "macaddr" or "macaddr8" => typeof(System.Net.NetworkInformation.PhysicalAddress),

                "box" => isNullable ? typeof(NpgsqlBox?) : typeof(NpgsqlBox),
                "circle" => isNullable ? typeof(NpgsqlCircle?) : typeof(NpgsqlCircle),
                "line" => isNullable ? typeof(NpgsqlLine?) : typeof(NpgsqlLine),
                "lseg" => isNullable ? typeof(NpgsqlLSeg?) : typeof(NpgsqlLSeg),
                "path" => isNullable ? typeof(NpgsqlPath?) : typeof(NpgsqlPath),
                "point" => isNullable ? typeof(NpgsqlPoint?) : typeof(NpgsqlPoint),
                "polygon" => isNullable ? typeof(NpgsqlPolygon?) : typeof(NpgsqlPolygon),

                _ => typeof(object)
            };
        }
    }
}
