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
        private readonly string[] systemDatabases = { "master", "model", "msdb", "tempdb" };
        private readonly string[] systemTables = { "VersionInfo" };

        private readonly SqlConnectionStringBuilder connectionStringBuilder;

        public MsSqlProvider(string connectionString)
            : base(connectionString)
        {
            connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
            connectionStringBuilder.InitialCatalog = "";
        }

        protected override IEnumerable<Database> GetDatabases()
        {
            DataTable databases = null;
            try
            {
                using (var db = new SqlConnection(connectionStringBuilder.ToString()))
                {
                    db.Open();
                    databases = db.GetSchema("Databases");
                    db.Close();
                }
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
                    if (!systemDatabases.Contains(databaseName))
                    {
                        var database = new Database
                        {
                            ConnectionType = DbConnectionTypes.MsSql,
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
                using (var db = new SqlConnection($"{connectionStringBuilder};Initial Catalog={databaseName};"))
                {
                    db.Open();
                    selectedDatabaseTables = db.GetSchema("Tables");
                    db.Close();
                }
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
                    if (!systemTables.Contains(tableName))
                    {
                        var table = new DatabaseTable
                        {
                            ConnectionType = DbConnectionTypes.MsSql,
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
            DataTable selectedDatabaseTablePrimaryColumns = null;
            try
            {
                using (var db = new SqlConnection($"{connectionStringBuilder};Initial Catalog={databaseName};"))
                {
                    db.Open();
                    selectedDatabaseTableColumns = db.GetSchema("Columns", new[] { databaseName, null, tableName });
                    selectedDatabaseTablePrimaryColumns = db.GetSchema("IndexColumns", new[] { databaseName, null, tableName });
                    db.Close();
                }
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

                    var primaryKey = false;
                    if (selectedDatabaseTablePrimaryColumns != null)
                    {
                        primaryKey = selectedDatabaseTablePrimaryColumns.Rows.Cast<DataRow>()
                            .Select(primaryColumnRow => primaryColumnRow.ItemArray[6].ToString())
                            .Any(primaryColumnName => primaryColumnName == columnName);
                    }
                    
                    var column = new DatabaseTableColumn
                    {
                        ConnectionType = DbConnectionTypes.MsSql,
                        DatabaseName = databaseName,
                        TableName = tableName,
                        ColumnName = columnName,
                        DataType = dataType,
                        Type = type,
                        TypeNamespace = type.Namespace,
                        MaxLength = maxLength,
                        PrimaryKey = primaryKey
                    };
                    yield return column;
                }
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
