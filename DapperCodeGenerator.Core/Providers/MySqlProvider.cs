using DapperCodeGenerator.Core.Enumerations;
using DapperCodeGenerator.Core.Models;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace DapperCodeGenerator.Core.Providers
{
    public class MySqlProvider(string connectionString) : Provider(connectionString)
    {
        private readonly string[] _systemDatabases = ["information_schema", "mysql", "performance_schema", "sys"];
        private readonly string[] _systemTables = [];

        private readonly MySqlConnectionStringBuilder _connectionStringBuilder = new(connectionString) { };

        protected override IEnumerable<Database> GetDatabases()
        {
            DataTable databases = null;
            try
            {
                using var db = new MySqlConnection(_connectionStringBuilder.ToString());
                db.Open();
                databases = db.GetSchema(SqlClientMetaDataCollectionNames.Databases);
                db.Close();
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message, exc);
            }

            if (databases == null)
            {
                yield break;
            }

            foreach (DataRow databaseRow in databases.Rows)
            {
                var databaseName = databaseRow.ItemArray[1].ToString();

                if (_systemDatabases.Any(d => d.Equals(databaseName, StringComparison.InvariantCultureIgnoreCase)))
                {
                    continue;
                }

                var database = new Database
                {
                    ConnectionType = DbConnectionTypes.MySql,
                    DatabaseName = databaseName
                };

                yield return database;
            }
        }

        protected override IEnumerable<DatabaseTable> GetDatabaseTables(string databaseName)
        {
            DataTable selectedDatabaseTables = null;
            try
            {
                using var db = new MySqlConnection($"{_connectionStringBuilder};Database={databaseName};");
                db.Open();
                //selectedDatabaseTables = db.GetSchema(SqlClientMetaDataCollectionNames.Tables);
                string[] restrictions = new string[4];
                restrictions[1] = databaseName;
                selectedDatabaseTables = db.GetSchema("Tables", restrictions);
                db.Close();
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message, exc);
            }

            if (selectedDatabaseTables == null)
            {
                yield break;
            }

            foreach (DataRow tableRow in selectedDatabaseTables.Rows)
            {
                var tableName = tableRow.ItemArray[2].ToString();

                if (_systemTables.Any(t => t.Equals(tableName, StringComparison.InvariantCultureIgnoreCase)))
                {
                    continue;
                }

                var table = new DatabaseTable
                {
                    ConnectionType = DbConnectionTypes.MySql,
                    DatabaseName = databaseName,
                    TableName = tableName
                };

                yield return table;
            }
        }

        protected override IEnumerable<DatabaseTableColumn> GetDatabaseTableColumns(string databaseName, string tableName)
        {
            DataTable selectedDatabaseTableColumns = null;

            DataTable selectedDatabaseTablePrimaryColumns = null;
            DataTable selectedDatabaseTableForeignKeyColumns = null;
            try
            {
                using var db = new MySqlConnection($"{_connectionStringBuilder};Database={databaseName};");
                db.Open();
                var columnRestrictions = new string[3];
                columnRestrictions[1] = databaseName;
                columnRestrictions[2] = tableName;

                selectedDatabaseTableColumns = db.GetSchema(SqlClientMetaDataCollectionNames.Columns, columnRestrictions);

                selectedDatabaseTablePrimaryColumns = db.GetSchema(SqlClientMetaDataCollectionNames.IndexColumns, columnRestrictions);
                selectedDatabaseTableForeignKeyColumns = db.GetSchema(SqlClientMetaDataCollectionNames.ForeignKeys, columnRestrictions);
                db.Close();
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc.Message, exc);
            }

            if (selectedDatabaseTableColumns == null)
            {
                yield break;
            }

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
                    ConnectionType = DbConnectionTypes.MySql,
                    DatabaseName = databaseName,
                    TableName = tableName,
                    ColumnName = columnName,
                    DataType = dataType,
                    Type = type,
                    TypeNamespace = type.Namespace,
                    MaxLength = maxLength
                };

                if (selectedDatabaseTablePrimaryColumns != null)
                {
                    foreach (DataRow indexColumnRow in selectedDatabaseTablePrimaryColumns.Rows)
                    {
                        var indexId = indexColumnRow[2].ToString();
                        var indexColumnName = indexColumnRow[6].ToString();

                        if (indexColumnName != columnName)
                        {
                            continue;
                        }

                        if (indexId.Contains("PK_", StringComparison.Ordinal))
                        {
                            column.PrimaryKeys.Add(indexId);
                        }
                        else
                        {
                            column.UniqueKeys.Add(indexId);
                        }
                    }
                }

                yield return column;
            }
        }

        protected override Type GetClrType(string dbTypeName, bool isNullable)
        {
            return dbTypeName switch
            {
                "bigint" => isNullable ? typeof(long?) : typeof(long),
                "binary" or "image" or "timestamp" or "varbinary" => typeof(byte[]),
                "bit" => isNullable ? typeof(bool?) : typeof(bool),
                "char" or "nchar" or "ntext" or "nvarchar" or "text" or "varchar" or "xml" => typeof(string),
                "datetime" or "smalldatetime" or "date" or "time" or "datetime2" => isNullable
                    ? typeof(DateTime?)
                    : typeof(DateTime),
                "decimal" or "money" or "smallmoney" => isNullable ? typeof(decimal?) : typeof(decimal),
                "float" => isNullable ? typeof(double?) : typeof(double),
                "int" => isNullable ? typeof(int?) : typeof(int),
                "real" => isNullable ? typeof(float?) : typeof(float),
                "uniqueidentifier" => isNullable ? typeof(Guid?) : typeof(Guid),
                "smallint" => isNullable ? typeof(short?) : typeof(short),
                "tinyint" => isNullable ? typeof(byte?) : typeof(byte),
                "structured" => typeof(DataTable),
                "datetimeoffset" => isNullable ? typeof(DateTimeOffset?) : typeof(DateTimeOffset),
                _ => typeof(object)
            };
        }
    }
}
