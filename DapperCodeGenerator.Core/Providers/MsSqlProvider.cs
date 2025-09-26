using Dapper;
using DapperCodeGenerator.Core.Enumerations;
using DapperCodeGenerator.Core.Models;
using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Types;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace DapperCodeGenerator.Core.Providers
{
    internal class MsSqlColumnData
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
        public bool? IsComputed { get; set; }
    }

    internal class MsSqlConstraint
    {
        public string ColumnName { get; set; } = string.Empty;
        public string ConstraintName { get; set; } = string.Empty;
        public string ConstraintType { get; set; } = string.Empty;
    }

    public class MsSqlProvider(string connectionString) : Provider(connectionString)
    {
        private readonly string[] _systemTables = ["VersionInfo", "database_firewall_rules"];

        private readonly SqlConnectionStringBuilder _connectionStringBuilder = new(connectionString) { InitialCatalog = "" };

        protected override IEnumerable<Database> GetDatabases()
        {
            try
            {
                using var db = new SqlConnection($"{_connectionStringBuilder}");

                const string databasesQuery = @"
                    SELECT name as DatabaseName
                    FROM sys.databases
                    WHERE state = 0
                      AND database_id > 4  -- Excludes master(1), tempdb(2), model(3), msdb(4)
                      AND is_read_only = 0";

                var databases = db.Query<Database>(databasesQuery);
                Console.WriteLine($"MS SQL databases query returned {databases.Count()} databases");
                return databases.Select(database =>
                {
                    database.ConnectionType = DbConnectionTypes.MsSql;
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
                using var db = new SqlConnection($"{_connectionStringBuilder};Initial Catalog={databaseName};");

                const string tablesQuery = @"
                    SELECT TABLE_NAME as TableName
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'";

                var tables = db.Query<DatabaseTable>(tablesQuery)
                    .Where(table => !_systemTables.Any(t => t.Equals(table.TableName, StringComparison.InvariantCultureIgnoreCase)));
                Console.WriteLine($"MS SQL tables query returned {tables.Count()} filtered tables for database {databaseName}");
                return tables.Select(table =>
                {
                    table.ConnectionType = DbConnectionTypes.MsSql;
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
                using var db = new SqlConnection($"{_connectionStringBuilder};Initial Catalog={databaseName};");
                
                const string columnsQuery = @"
                    SELECT
                        c.COLUMN_NAME as ColumnName,
                        c.IS_NULLABLE as IsNullable,
                        c.DATA_TYPE as DataType,
                        c.CHARACTER_MAXIMUM_LENGTH as CharacterMaximumLength,
                        c.NUMERIC_PRECISION as NumericPrecision,
                        c.NUMERIC_SCALE as NumericScale,
                        c.COLUMN_DEFAULT as ColumnDefault,
                        c.ORDINAL_POSITION as OrdinalPosition,
                        ic.is_identity as IsIdentity,
                        cc.is_computed as IsComputed
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    LEFT JOIN sys.tables t ON t.name = c.TABLE_NAME
                    LEFT JOIN sys.columns ic ON ic.object_id = t.object_id AND ic.name = c.COLUMN_NAME
                    LEFT JOIN sys.computed_columns cc ON cc.object_id = t.object_id AND cc.name = c.COLUMN_NAME
                    WHERE c.TABLE_NAME = @tableName
                    ORDER BY c.ORDINAL_POSITION";
                var mssqlColumns = db.Query<MsSqlColumnData>(columnsQuery, new { tableName });
                Console.WriteLine($"MS SQL columns query returned {mssqlColumns.Count()} columns for table {tableName} in database {databaseName}");

                const string constraintsQuery = @"
                    SELECT
                        kcu.COLUMN_NAME as ColumnName,
                        tc.CONSTRAINT_NAME as ConstraintName,
                        tc.CONSTRAINT_TYPE as ConstraintType
                    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                        ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                        AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                        AND tc.TABLE_NAME = kcu.TABLE_NAME
                    WHERE tc.TABLE_NAME = @tableName
                        AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
                    ORDER BY kcu.ORDINAL_POSITION";
                var mssqlConstraints = db.Query<MsSqlConstraint>(constraintsQuery, new { tableName });
                Console.WriteLine($"MS SQL constraints query returned {mssqlConstraints.Count()} constraints for table {tableName} in database {databaseName}");

                foreach (var mssqlColumn in mssqlColumns)
                {
                    var isNullable = mssqlColumn.IsNullable == "YES";
                    var type = GetClrType(mssqlColumn.DataType, isNullable);
                    var maxLength = mssqlColumn.CharacterMaximumLength ?? 0;

                    var column = new DatabaseTableColumn
                    {
                        ConnectionType = DbConnectionTypes.MsSql,
                        DatabaseName = databaseName,
                        TableName = tableName,
                        ColumnName = mssqlColumn.ColumnName,
                        DataType = mssqlColumn.DataType,
                        Type = type,
                        TypeNamespace = type.Namespace,
                        MaxLength = maxLength,
                        NumericPrecision = mssqlColumn.NumericPrecision,
                        NumericScale = mssqlColumn.NumericScale,
                        DefaultValue = mssqlColumn.ColumnDefault,
                        OrdinalPosition = mssqlColumn.OrdinalPosition,
                        IsNullable = isNullable,
                        IsAutoIncrement = mssqlColumn.IsIdentity,
                        IsComputed = mssqlColumn.IsComputed,
                        IsGenerated = mssqlColumn.IsComputed
                    };

                    foreach (var constraint in mssqlConstraints.Where(c => c.ColumnName == mssqlColumn.ColumnName))
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

        protected override Type GetClrType(string dbTypeName, bool isNullable)
        {
            var lowerTypeName = dbTypeName.ToLowerInvariant();

            return lowerTypeName switch
            {
                "bit" => isNullable ? typeof(bool?) : typeof(bool),
                "tinyint" => isNullable ? typeof(byte?) : typeof(byte),
                "smallint" => isNullable ? typeof(short?) : typeof(short),
                "int" => isNullable ? typeof(int?) : typeof(int),
                "bigint" => isNullable ? typeof(long?) : typeof(long),
                "real" => isNullable ? typeof(float?) : typeof(float),
                "float" => isNullable ? typeof(double?) : typeof(double),
                "decimal" or "numeric" => isNullable ? typeof(decimal?) : typeof(decimal),
                "money" or "smallmoney" => isNullable ? typeof(decimal?) : typeof(decimal),

                "char" or "nchar" or "varchar" or "nvarchar" => typeof(string),
                "xml" => typeof(string),

                "date" => isNullable ? typeof(DateOnly?) : typeof(DateOnly),
                "time" => isNullable ? typeof(TimeOnly?) : typeof(TimeOnly),
                "datetime" or "datetime2" or "smalldatetime" => isNullable ? typeof(DateTime?) : typeof(DateTime),
                "datetimeoffset" => isNullable ? typeof(DateTimeOffset?) : typeof(DateTimeOffset),

                "uniqueidentifier" => isNullable ? typeof(Guid?) : typeof(Guid),

                "binary" or "varbinary" => typeof(byte[]),
                "rowversion" or "timestamp" => typeof(byte[]),

                "structured" => typeof(DataTable),

                "geography" => typeof(SqlGeography),
                "geometry" => typeof(SqlGeometry),
                "hierarchyid" => typeof(SqlHierarchyId),

                "image" => typeof(byte[]), // deprecated: use varbinary(max)
                "ntext" or "text" => typeof(string), // deprecated: use nvarchar(max)/varchar(max)

                // "cursor" => typeof(object),
                // "sql_variant" => typeof(object),
                _ => typeof(object)
            };
        }
    }
}
