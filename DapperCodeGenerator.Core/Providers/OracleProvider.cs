using Dapper;
using DapperCodeGenerator.Core.Enumerations;
using DapperCodeGenerator.Core.Models;
using DapperCodeGenerator.Core.Providers.Oracle;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DapperCodeGenerator.Core.Providers
{
    public class OracleProvider : Provider
    {
        private readonly OracleConnectionStringBuilder _connectionStringBuilder;

        public OracleProvider(string connectionString)
            : base(connectionString)
        {
            _connectionStringBuilder = new OracleConnectionStringBuilder(connectionString);

            DefaultTypeMap.MatchNamesWithUnderscores = true;
        }

        protected override IEnumerable<Database> GetDatabases()
        {
            var databases = new List<Database>();

            try
            {
                using var db = new OracleConnection($"{_connectionStringBuilder}");
                // NOTE: this will include all "Users" AND "Schemas"
                const string oracleSchemasQuery = @"
                        SELECT
                            USERNAME AS SCHEMA_NAME
                        FROM ALL_USERS
                        ORDER BY USERNAME";
                var oracleSchemas = db.Query<OracleSchema>(oracleSchemasQuery);
                foreach (var oracleSchema in oracleSchemas)
                {
                    var database = new Database
                    {
                        ConnectionType = DbConnectionTypes.Oracle,
                        DatabaseName = oracleSchema.SchemaName
                    };
                    databases.Add(database);
                }
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message, exc);
            }

            return databases;
        }

        protected override IEnumerable<DatabaseTable> GetDatabaseTables(string databaseName)
        {
            var tables = new List<DatabaseTable>();

            try
            {
                using var db = new OracleConnection($"{_connectionStringBuilder}");
                const string oracleTablesQuery = @"
                        SELECT DISTINCT
                            OBJECT_NAME AS TABLE_NAME
                        FROM ALL_OBJECTS
                        WHERE
                            OBJECT_TYPE = 'TABLE' AND
                            OWNER = :schemaName";
                var oracleTables = db.Query<OracleTable>(oracleTablesQuery, new { schemaName = databaseName });
                foreach (var oracleTable in oracleTables)
                {
                    var table = new DatabaseTable
                    {
                        ConnectionType = DbConnectionTypes.Oracle,
                        DatabaseName = databaseName,
                        TableName = oracleTable.TableName
                    };
                    tables.Add(table);
                }
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message, exc);
            }

            return tables;
        }

        protected override IEnumerable<DatabaseTableColumn> GetDatabaseTableColumns(string databaseName, string tableName)
        {
            var columns = new List<DatabaseTableColumn>();

            try
            {
                using var db = new OracleConnection($"{_connectionStringBuilder}");
                const string oracleColumnsQuery = @"
                        SELECT
                            COLUMN_NAME,
                            DATA_TYPE,
                            DATA_LENGTH,
                            NULLABLE
                        FROM ALL_TAB_COLUMNS
                        WHERE TABLE_NAME = :tableName";
                var oracleColumns = db.Query<OracleColumn>(oracleColumnsQuery, new { tableName });
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
                        MaxLength = dataLength
                    };
                    columns.Add(column);
                }

                const string columnConstraintsQuery = @"
                        SELECT
                            COLUMN_NAME,
                            CONSTRAINT_NAME,
                            CONSTRAINT_TYPE
                        FROM ALL_CONSTRAINTS
                        NATURAL JOIN ALL_CONS_COLUMNS
                        WHERE TABLE_NAME = :tableName";
                var columnConstraints = db.Query<OracleConstraint>(columnConstraintsQuery, new { tableName });
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
            if (dbTypeName.StartsWith("TIMESTAMP"))
            {
                return isNullable ? typeof(DateTime?) : typeof(DateTime);
            }

            return dbTypeName switch
            {
                "INTERVAL YEAR TO MONTH" => isNullable ? typeof(long?) : typeof(long),
                "BFILE" or "BLOB" or "LONG RAW" => typeof(byte[]),
                "RAW" => typeof(Guid),
                "BIT" => isNullable ? typeof(bool?) : typeof(bool),
                "CHAR" or "CLOB" or "LONG" or "NCHAR" or "NCLOB" or "REF" or "ROWID" or "UROWID" or "VARCHAR2"
                    or "NVARCHAR2" or "XMLType" => typeof(string),
                "DATE" => isNullable ? typeof(DateTime?) : typeof(DateTime),
                "BINARY_DOUBLE" or "BINARY_FLOAT" or "BINARY_INTEGER" or "NUMBER" or "PLS_INTEGER" or "FLOAT" =>
                    isNullable ? typeof(decimal?) : typeof(decimal),
                _ => typeof(object)
            };
        }
    }
}
