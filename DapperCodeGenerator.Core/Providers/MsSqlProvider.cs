using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using DapperCodeGenerator.Core.Enumerations;
using DapperCodeGenerator.Core.Models;

namespace DapperCodeGenerator.Core.Providers
{
    public class MsSqlProvider : Provider
    {
        private readonly string[] _systemDatabases = { "master", "model", "msdb", "tempdb" };
        private readonly string[] _systemTables = { "VersionInfo", "database_firewall_rules" };

        private readonly SqlConnectionStringBuilder _connectionStringBuilder;

        public MsSqlProvider(string connectionString)
            : base(connectionString)
        {
            _connectionStringBuilder = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = "" };
        }

        protected override IEnumerable<Database> GetDatabases()
        {
            DataTable databases = null;
            try
            {
                using var db = new SqlConnection(_connectionStringBuilder.ToString());
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
                var databaseName = databaseRow.ItemArray[0].ToString();

                if (_systemDatabases.Any(d => d.Equals(databaseName, StringComparison.InvariantCultureIgnoreCase)))
                {
                    continue;
                }

                var database = new Database
                {
                    ConnectionType = DbConnectionTypes.MsSql,
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
                using var db = new SqlConnection($"{_connectionStringBuilder};Initial Catalog={databaseName};");
                db.Open();
                selectedDatabaseTables = db.GetSchema(SqlClientMetaDataCollectionNames.Tables);
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
                    ConnectionType = DbConnectionTypes.MsSql,
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
                using var db = new SqlConnection($"{_connectionStringBuilder};Initial Catalog={databaseName};");
                db.Open();
                var columnRestrictions = new string[3];
                columnRestrictions[0] = databaseName;
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
                    ConnectionType = DbConnectionTypes.MsSql,
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

                        if (indexId.IndexOf("PK_", StringComparison.Ordinal) != -1)
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
            switch (dbTypeName)
            {
                case "bigint":
                    return isNullable ? typeof(long?) : typeof(long);

                case "binary":
                case "image":
                case "timestamp":
                case "varbinary":
                    return typeof(byte[]);

                case "bit":
                    return isNullable ? typeof(bool?) : typeof(bool);

                case "char":
                case "nchar":
                case "ntext":
                case "nvarchar":
                case "text":
                case "varchar":
                case "xml":
                    return typeof(string);

                case "datetime":
                case "smalldatetime":
                case "date":
                case "time":
                case "datetime2":
                    return isNullable ? typeof(DateTime?) : typeof(DateTime);

                case "decimal":
                case "money":
                case "smallmoney":
                    return isNullable ? typeof(decimal?) : typeof(decimal);

                case "float":
                    return isNullable ? typeof(double?) : typeof(double);

                case "int":
                    return isNullable ? typeof(int?) : typeof(int);

                case "real":
                    return isNullable ? typeof(float?) : typeof(float);

                case "uniqueidentifier":
                    return isNullable ? typeof(Guid?) : typeof(Guid);

                case "smallint":
                    return isNullable ? typeof(short?) : typeof(short);

                case "tinyint":
                    return isNullable ? typeof(byte?) : typeof(byte);

                case "structured":
                    return typeof(DataTable);

                case "datetimeoffset":
                    return isNullable ? typeof(DateTimeOffset?) : typeof(DateTimeOffset);

                default:
                    return typeof(object);
            }
        }
    }
}
