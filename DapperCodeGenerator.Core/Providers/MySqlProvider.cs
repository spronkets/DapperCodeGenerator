using Dapper;
using DapperCodeGenerator.Core.Enumerations;
using DapperCodeGenerator.Core.Models;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace DapperCodeGenerator.Core.Providers
{
    internal class MySqlColumnData
    {
        public string ColumnName { get; set; } = string.Empty;
        public string IsNullable { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public long? CharacterMaximumLength { get; set; }
        public long? NumericPrecision { get; set; }
        public long? NumericScale { get; set; }
        public string ColumnDefault { get; set; }
        public long OrdinalPosition { get; set; }
        public string Extra { get; set; }  // Contains auto_increment and other MySQL-specific info
        public string ColumnKey { get; set; }  // PRI, UNI, MUL, etc.
    }

    internal class MySqlConstraint
    {
        public string ColumnName { get; set; } = string.Empty;
        public string ConstraintName { get; set; } = string.Empty;
        public string ConstraintType { get; set; } = string.Empty;
    }

    public class MySqlProvider(string connectionString) : Provider(connectionString)
    {
        private readonly string[] _systemDatabases = ["information_schema", "mysql", "performance_schema", "sys"];
        private readonly string[] _systemTables = ["__MigrationHistory", "sysdiagrams"];

        private readonly MySqlConnectionStringBuilder _connectionStringBuilder = new(connectionString);

        protected override IEnumerable<Database> GetDatabases()
        {
            try
            {
                using var db = new MySqlConnection($"{_connectionStringBuilder}");

                const string databasesQuery = @"
                    SELECT SCHEMA_NAME as DatabaseName
                    FROM information_schema.SCHEMATA";

                var databases = db.Query<Database>(databasesQuery)
                    .Where(database => !_systemDatabases.Any(d => d.Equals(database.DatabaseName, StringComparison.InvariantCultureIgnoreCase)));
                Console.WriteLine($"MySQL databases query returned {databases.Count()} filtered databases");
                return databases.Select(database =>
                {
                    database.ConnectionType = DbConnectionTypes.MySql;
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
                using var db = new MySqlConnection($"{_connectionStringBuilder};Database={databaseName};");

                const string tablesQuery = @"
                    SELECT TABLE_NAME as TableName
                    FROM information_schema.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                        AND TABLE_SCHEMA = @schema";

                var tables = db.Query<DatabaseTable>(tablesQuery, new { schema = databaseName })
                    .Where(table => !_systemTables.Any(t => t.Equals(table.TableName, StringComparison.InvariantCultureIgnoreCase)));
                Console.WriteLine($"MySQL tables query returned {tables.Count()} filtered tables for database {databaseName}");
                return tables.Select(table =>
                {
                    table.ConnectionType = DbConnectionTypes.MySql;
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
            List<MySqlColumnData> selectedDatabaseTableColumns = null;
            List<MySqlConstraint> selectedDatabaseTableConstraints = null;

            try
            {
                using var db = new MySqlConnection($"{_connectionStringBuilder};Database={databaseName};");

                const string columnsQuery = @"
                    SELECT
                        COLUMN_NAME as ColumnName,
                        IS_NULLABLE as IsNullable,
                        DATA_TYPE as DataType,
                        CHARACTER_MAXIMUM_LENGTH as CharacterMaximumLength,
                        NUMERIC_PRECISION as NumericPrecision,
                        NUMERIC_SCALE as NumericScale,
                        COLUMN_DEFAULT as ColumnDefault,
                        ORDINAL_POSITION as OrdinalPosition,
                        EXTRA as Extra,
                        COLUMN_KEY as ColumnKey
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = @schema
                        AND TABLE_NAME = @table
                    ORDER BY ORDINAL_POSITION";

                const string constraintsQuery = @"
                    SELECT
                        kcu.COLUMN_NAME as ColumnName,
                        kcu.CONSTRAINT_NAME as ConstraintName,
                        tc.CONSTRAINT_TYPE as ConstraintType
                    FROM information_schema.KEY_COLUMN_USAGE kcu
                    JOIN information_schema.TABLE_CONSTRAINTS tc
                        ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                        AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
                        AND kcu.TABLE_NAME = tc.TABLE_NAME
                    WHERE kcu.TABLE_SCHEMA = @schema
                        AND kcu.TABLE_NAME = @table
                        AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
                    ORDER BY kcu.ORDINAL_POSITION";

                selectedDatabaseTableColumns = [.. db.Query<MySqlColumnData>(columnsQuery, new { schema = databaseName, table = tableName })];
                Console.WriteLine($"MySQL columns query returned {selectedDatabaseTableColumns.Count} columns for table {tableName} in database {databaseName}");
                selectedDatabaseTableConstraints = [.. db.Query<MySqlConstraint>(constraintsQuery, new { schema = databaseName, table = tableName })];
                Console.WriteLine($"MySQL constraints query returned {selectedDatabaseTableConstraints.Count} constraints for table {tableName} in database {databaseName}");
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message, exc);
            }

            var columns = new List<DatabaseTableColumn>();

            if (selectedDatabaseTableColumns != null)
            {
                foreach (var mysqlColumn in selectedDatabaseTableColumns)
                {
                    var isNullable = mysqlColumn.IsNullable == "YES";
                    var type = GetClrType(mysqlColumn.DataType, isNullable);
                    var maxLength = (int)(mysqlColumn.CharacterMaximumLength ?? 0);

                    var column = new DatabaseTableColumn
                    {
                        ConnectionType = DbConnectionTypes.MySql,
                        DatabaseName = databaseName,
                        TableName = tableName,
                        ColumnName = mysqlColumn.ColumnName,
                        DataType = mysqlColumn.DataType,
                        Type = type,
                        TypeNamespace = type.Namespace,
                        MaxLength = maxLength,
                        NumericPrecision = (int?)mysqlColumn.NumericPrecision,
                        NumericScale = (int?)mysqlColumn.NumericScale,
                        DefaultValue = mysqlColumn.ColumnDefault,
                        OrdinalPosition = (int)mysqlColumn.OrdinalPosition,
                        IsNullable = isNullable,
                        IsAutoIncrement = string.IsNullOrEmpty(mysqlColumn.Extra) ? null : mysqlColumn.Extra.Contains("auto_increment"),
                        IsComputed = false,
                        IsGenerated = string.IsNullOrEmpty(mysqlColumn.Extra) ? null : mysqlColumn.Extra.Contains("GENERATED") || mysqlColumn.Extra.Contains("VIRTUAL")
                    };

                    if (selectedDatabaseTableConstraints != null)
                    {
                        foreach (var constraint in selectedDatabaseTableConstraints.Where(c => c.ColumnName == mysqlColumn.ColumnName))
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
                    }

                    columns.Add(column);
                }
            }

            return columns;
        }

        protected override Type GetClrType(string dbTypeName, bool isNullable)
        {
            var lowerTypeName = dbTypeName.ToLowerInvariant();

            return lowerTypeName switch
            {
                "bit" => isNullable ? typeof(bool?) : typeof(bool),
                "tinyint" => isNullable ? typeof(sbyte?) : typeof(sbyte),
                "tinyint unsigned" => isNullable ? typeof(byte?) : typeof(byte),
                "smallint" => isNullable ? typeof(short?) : typeof(short),
                "smallint unsigned" => isNullable ? typeof(ushort?) : typeof(ushort),
                "int" or "integer" => isNullable ? typeof(int?) : typeof(int),
                "int unsigned" or "integer unsigned" => isNullable ? typeof(uint?) : typeof(uint),
                "mediumint" => isNullable ? typeof(int?) : typeof(int),
                "mediumint unsigned" => isNullable ? typeof(uint?) : typeof(uint),
                "year" => isNullable ? typeof(int?) : typeof(int),
                "bigint" => isNullable ? typeof(long?) : typeof(long),
                "bigint unsigned" => isNullable ? typeof(ulong?) : typeof(ulong),
                "float" or "real" => isNullable ? typeof(float?) : typeof(float),
                "double" or "double precision" => isNullable ? typeof(double?) : typeof(double),
                "decimal" or "fixed" or "numeric" => isNullable ? typeof(decimal?) : typeof(decimal),

                "char" or "varchar" => typeof(string),
                "longtext" or "mediumtext" or "text" or "tinytext" => typeof(string),
                "enum" or "set" => typeof(string),
                "json" => typeof(string),

                "date" => isNullable ? typeof(DateOnly?) : typeof(DateOnly),
                "time" => isNullable ? typeof(TimeOnly?) : typeof(TimeOnly),
                "datetime" => isNullable ? typeof(DateTime?) : typeof(DateTime),
                "timestamp" => isNullable ? typeof(DateTime?) : typeof(DateTime),

                "binary" or "varbinary" => typeof(byte[]),
                "blob" or "longblob" or "mediumblob" or "tinyblob" => typeof(byte[]),
                "geometrycollection" or "geometry" or "linestring" or "multilinestring" or "multipoint" or "multipolygon" or "point" or "polygon" => typeof(byte[]),

                _ => typeof(object)
            };
        }
    }
}
