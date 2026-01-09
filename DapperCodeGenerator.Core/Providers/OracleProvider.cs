using Dapper;
using DapperCodeGenerator.Core.Enumerations;
using DapperCodeGenerator.Core.Models;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace DapperCodeGenerator.Core.Providers
{
    internal class OracleSchema
    {
        public string SchemaName { get; set; } = string.Empty;
    }

    internal class OracleColumnData
    {
        public string ColumnName { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public string DataLength { get; set; } = string.Empty;
        public string Nullable { get; set; } = string.Empty;
        public int? DataPrecision { get; set; }
        public int? DataScale { get; set; }
        public string DataDefault { get; set; }
        public int ColumnId { get; set; }
        public bool? IsIdentity { get; set; }
        public bool? IsGenerated { get; set; }
    }

    internal class OracleConstraint
    {
        public string ColumnName { get; set; } = string.Empty;
        public string ConstraintName { get; set; } = string.Empty;
        public string ConstraintType { get; set; } = string.Empty;
    }

    public class OracleProvider(string connectionString) : Provider(connectionString)
    {
        private readonly string[] _systemDatabases = [
            "ANONYMOUS", "APPQOSSYS", "AUDSYS", "CTXSYS", "DBSFWUSER", "DBSNMP",
            "DGPDB_INT", "DIP", "DVF", "DVSYS", "GGSYS", "GSMADMIN_INTERNAL",
            "GSMCATUSER", "GSMROOTUSER", "GSMUSER", "LBACSYS", "MDDATA", "MDSYS",
            "OPS$ORACLE", "ORACLE_OCM", "OUTLN", "REMOTE_SCHEDULER_AGENT", "SYS",
            "SYS$UMF", "SYSBACKUP", "SYSDG", "SYSKM", "SYSRAC", "XDB", "XS$NULL"
        ];
        private readonly string[] _systemTablePatterns = [
            "^APPLY", "^AQ", "^BIN", "^CAPTURE", "^DEF", "^HELP$", "^LOGMNR", "^LOGMNRC",
            "^LOGSTDBY", "^MLOG", "^MVIEW", "^OL", "^PROPAGATION", "^REDO_", "^REPL_",
            "^ROLLING", "^RUPD", "^SCHEDULER", "^SCHEDULER_", "^SQLPLUS_", "^STREAMS",
            "^USLOG", "^WRH", "^WRI", "^WRR"
        ];

        private readonly OracleConnectionStringBuilder _connectionStringBuilder = new(connectionString);
        private Version _oracleVersion;

        protected override IEnumerable<Database> GetDatabases(bool filterSystemObjects)
        {
            try
            {
                using var db = new OracleConnection($"{_connectionStringBuilder}");

                // NOTE: this will include all "Users" AND "Schemas"
                string databasesQuery = @"
                    SELECT
                        USERNAME as SchemaName
                    FROM ALL_USERS
                    ORDER BY USERNAME";

                var schemas = db.Query<OracleSchema>(databasesQuery);

                if (filterSystemObjects)
                {
                    schemas = schemas.Where(schema => !_systemDatabases.Any(d => d.Equals(schema.SchemaName, StringComparison.InvariantCultureIgnoreCase)));
                }

                Console.WriteLine($"Oracle databases query returned {schemas.Count()} {(filterSystemObjects ? "filtered" : "")} schemas");
                return schemas.Select(schema => new Database
                {
                    ConnectionType = DbConnectionTypes.Oracle,
                    DatabaseName = schema.SchemaName
                });
            }
            catch (Exception exc)
            {
                LastConnectionError = exc.Message;
                Console.Error.WriteLine(exc.Message, exc);
                return [];
            }
        }

        protected override IEnumerable<DatabaseTable> GetDatabaseTables(string databaseName, bool filterSystemObjects)
        {
            try
            {
                using var db = new OracleConnection($"{_connectionStringBuilder}");

                var tablesQuery = filterSystemObjects
                    ? @"SELECT DISTINCT
                        OBJECT_NAME as TableName
                        FROM ALL_OBJECTS
                        WHERE
                            OBJECT_TYPE = 'TABLE' AND
                            OWNER = :schemaName AND
                            GENERATED = 'N' AND
                            SECONDARY = 'N' AND
                            OBJECT_NAME NOT LIKE 'BIN$%'"
                    : @"SELECT DISTINCT
                        OBJECT_NAME as TableName
                        FROM ALL_OBJECTS
                        WHERE
                            OBJECT_TYPE = 'TABLE' AND
                            OWNER = :schemaName";

                var tables = db.Query<DatabaseTable>(tablesQuery, new { schemaName = databaseName });

                if (filterSystemObjects)
                {
                    tables = tables.Where(table => !_systemTablePatterns.Any(pattern => Regex.IsMatch(table.TableName, pattern, RegexOptions.IgnoreCase)));
                }

                Console.WriteLine($"Oracle tables query returned {tables.Count()} {(filterSystemObjects ? "filtered" : "")} tables for schema {databaseName}");
                return tables.Select(table =>
                {
                    table.ConnectionType = DbConnectionTypes.Oracle;
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
                using var db = new OracleConnection($"{_connectionStringBuilder}");

                var version = GetOracleVersion(db);

                var columnsQuery = BuildColumnQuery(version);

                var oracleColumns = db.Query<OracleColumnData>(columnsQuery, new { tableName, schemaName = databaseName });
                Console.WriteLine($"Oracle columns query returned {oracleColumns.Count()} results for table {tableName} in schema {databaseName}");
                foreach (var oracleColumn in oracleColumns)
                {
                    var nullable = oracleColumn.Nullable == "Y";
                    var dataType = GetClrType(oracleColumn.DataType, nullable);
                    int.TryParse(oracleColumn.DataLength, out var dataLength);

                    var column = new DatabaseTableColumn
                    {
                        ConnectionType = DbConnectionTypes.Oracle,
                        DatabaseName = databaseName,
                        TableName = tableName,
                        ColumnName = oracleColumn.ColumnName,
                        DataType = oracleColumn.DataType,
                        Type = dataType,
                        TypeNamespace = dataType.Namespace,
                        MaxLength = dataLength,
                        NumericPrecision = oracleColumn.DataPrecision,
                        NumericScale = oracleColumn.DataScale,
                        DefaultValue = oracleColumn.DataDefault,
                        OrdinalPosition = oracleColumn.ColumnId,
                        IsNullable = nullable,
                        IsAutoIncrement = oracleColumn.IsIdentity,
                        IsComputed = false,
                        IsGenerated = oracleColumn.IsGenerated
                    };
                    columns.Add(column);
                }

                const string constraintsQuery = @"
                    SELECT
                        cc.COLUMN_NAME as ColumnName,
                        c.CONSTRAINT_NAME as ConstraintName,
                        c.CONSTRAINT_TYPE as ConstraintType
                    FROM ALL_CONSTRAINTS c
                    JOIN ALL_CONS_COLUMNS cc ON c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME AND c.OWNER = cc.OWNER
                    WHERE c.TABLE_NAME = :tableName AND c.OWNER = :schemaName";
                var columnConstraints = db.Query<OracleConstraint>(constraintsQuery, new { tableName, schemaName = databaseName });
                Console.WriteLine($"Oracle constraints query returned {columnConstraints.Count()} constraints for table {tableName} in schema {databaseName}");
                foreach (var columnConstraint in columnConstraints)
                {
                    var columnName = columnConstraint.ColumnName;

                    var column = columns.SingleOrDefault(c => c.TableName == tableName && c.ColumnName == columnName);
                    if (column != null)
                    {
                        var constraintName = columnConstraint.ConstraintName;
                        var constraintType = columnConstraint.ConstraintType;

                        switch (constraintType)
                        {
                            case "P": // Primary Key
                                column.PrimaryKeys.Add(constraintName);
                                break;
                            case "R": // Foreign Key
                                column.ForeignKeys.Add(constraintName);
                                break;
                            case "U": // Unique Key
                                column.UniqueKeys.Add(constraintName);
                                break;
                            case "C": // Check on a Table
                            case "O": // Read Only on a View
                            case "V": // Check Option on a View
                            default:
                                // Do nothing
                                break;
                        }
                    }
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
            var upperTypeName = dbTypeName.ToUpperInvariant();

            if (upperTypeName.StartsWith("TIMESTAMP"))
            {
                if (upperTypeName.Contains("TIME ZONE"))
                {
                    return isNullable ? typeof(DateTimeOffset?) : typeof(DateTimeOffset);
                }

                return isNullable ? typeof(DateTime?) : typeof(DateTime);
            }

            return upperTypeName switch
            {
                "BOOLEAN" => isNullable ? typeof(bool?) : typeof(bool),
                "SMALLINT" => isNullable ? typeof(short?) : typeof(short),
                "INT" or "INTEGER" => isNullable ? typeof(int?) : typeof(int),
                "BINARY_INTEGER" or "PLS_INTEGER" => isNullable ? typeof(int?) : typeof(int),
                "BINARY_FLOAT" => isNullable ? typeof(float?) : typeof(float),
                "BINARY_DOUBLE" => isNullable ? typeof(double?) : typeof(double),
                "FLOAT" => isNullable ? typeof(decimal?) : typeof(decimal),
                "NUMBER" => isNullable ? typeof(decimal?) : typeof(decimal),

                "CHAR" or "NCHAR" => typeof(string),
                "NVARCHAR2" or "VARCHAR2" => typeof(string),
                "CLOB" or "NCLOB" => typeof(string),
                "JSON" => typeof(string),
                "REF" => typeof(string),
                "ROWID" or "UROWID" => typeof(string),
                "XMLTYPE" => typeof(string),

                "DATE" => isNullable ? typeof(DateTime?) : typeof(DateTime),
                "INTERVAL DAY TO SECOND" or "INTERVAL YEAR TO MONTH" => isNullable ? typeof(TimeSpan?) : typeof(TimeSpan),

                "BFILE" => typeof(byte[]),
                "BLOB" => typeof(byte[]),
                "LONG RAW" or "RAW" => typeof(byte[]),

                "SDO_ELEM_INFO_ARRAY" or "SDO_ORDINATE_ARRAY" => typeof(decimal[]),

                "LONG" => typeof(string), // deprecated: use CLOB instead

                // "ANYDATA" or "ANYDATASET" or "ANYTYPE" => typeof(object),
                // "SDO_GEOMETRY" or "SDO_POINT_TYPE" => typeof(object),
                _ => typeof(object)
            };
        }

        private Version GetOracleVersion(OracleConnection connection)
        {
            if (_oracleVersion != null)
            {
                return _oracleVersion;
            }

            try
            {
                var versionInfo = connection.Query<string>("SELECT banner FROM v$version WHERE ROWNUM = 1").FirstOrDefault();
                Console.WriteLine($"Oracle version detected: {versionInfo}");
                if (!string.IsNullOrEmpty(versionInfo))
                {
                    var match = System.Text.RegularExpressions.Regex.Match(versionInfo, @"Release (\d+)\.(\d+)\.(\d+)");
                    if (match.Success)
                    {
                        _oracleVersion = new Version(int.Parse(match.Groups[1].Value), int.Parse(match.Groups[2].Value), int.Parse(match.Groups[3].Value));
                        return _oracleVersion;
                    }
                }
            }
            catch
            {
                // Ignore
            }

            _oracleVersion = new Version(11, 0, 0);
            return _oracleVersion;
        }

        private static string BuildColumnQuery(Version oracleVersion)
        {
            var baseQuery = @"
                SELECT
                    COLUMN_NAME as ColumnName,
                    DATA_TYPE as DataType,
                    DATA_LENGTH as DataLength,
                    NULLABLE as Nullable,
                    DATA_PRECISION as DataPrecision,
                    DATA_SCALE as DataScale,
                    DATA_DEFAULT as DataDefault,
                    COLUMN_ID as ColumnId";

            // Add version-specific columns
            if (oracleVersion >= new Version(12, 1)) // Identity columns introduced in 12c
            {
                baseQuery += @",
                    CASE WHEN IDENTITY_COLUMN = 'YES' THEN 1
                         WHEN IDENTITY_COLUMN = 'NO' THEN 0
                         ELSE null END as IsIdentity";
            }
            else
            {
                baseQuery += ", null as IsIdentity";
            }

            // TODO: VIRTUAL_COLUMN?
            baseQuery += ", null as IsGenerated";

            baseQuery += @"
                FROM ALL_TAB_COLUMNS
                WHERE TABLE_NAME = :tableName AND OWNER = :schemaName
                ORDER BY COLUMN_ID";

            return baseQuery;
        }
    }
}
