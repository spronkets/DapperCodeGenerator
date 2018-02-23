using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using DapperCodeGenerator.Core.Enumerations;
using DapperCodeGenerator.Core.Models;
using Oracle.ManagedDataAccess.Client;

namespace DapperCodeGenerator.Core.Providers
{
    public class OracleProvider : Provider
    {
        private readonly string[] systemDatabases = { };
        private readonly string[] systemTables = { };

        private readonly OracleConnectionStringBuilder connectionStringBuilder;

        public OracleProvider(string connectionString)
            : base(connectionString)
        {
            connectionStringBuilder = new OracleConnectionStringBuilder(connectionString);
        }

        protected override IEnumerable<Database> GetDatabases()
        {
            DataTable databases = null;
            try
            {
                using (var db = new OracleConnection($"{connectionStringBuilder};Password={connectionStringBuilder.Password};"))
                {
                    db.Open();
                    databases = db.GetSchema(SqlClientMetaDataCollectionNames.Databases);
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
                    if (!systemDatabases.Any(d => d.Equals(databaseName, StringComparison.InvariantCultureIgnoreCase)))
                    {
                        var database = new Database
                        {
                            ConnectionType = DbConnectionTypes.Oracle,
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
                using (var db = new OracleConnection($"{connectionStringBuilder};Password={connectionStringBuilder.Password};Initial Catalog={databaseName};"))
                {
                    db.Open();
                    selectedDatabaseTables = db.GetSchema(SqlClientMetaDataCollectionNames.Tables);
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
                            ConnectionType = DbConnectionTypes.Oracle,
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
                using (var db = new OracleConnection($"{connectionStringBuilder};Password={connectionStringBuilder.Password};Initial Catalog={databaseName};"))
                {
                    db.Open();
                    var columnRestrictions = new string[3];
                    columnRestrictions[0] = databaseName;
                    columnRestrictions[2] = tableName;

                    selectedDatabaseTableColumns = db.GetSchema(SqlClientMetaDataCollectionNames.Columns, columnRestrictions);

                    selectedDatabaseTablePrimaryColumns = db.GetSchema(SqlClientMetaDataCollectionNames.IndexColumns, columnRestrictions);
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

                    var column = new DatabaseTableColumn
                    {
                        ConnectionType = DbConnectionTypes.Oracle,
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

                            if (indexColumnName == columnName)
                            {
                                if (indexId.StartsWith("pk_", StringComparison.InvariantCultureIgnoreCase))
                                {
                                    column.PrimaryKeys.Add(indexId);
                                }
                                else
                                {
                                    column.UniqueKeys.Add(indexId);
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
            switch (dbTypeName)
            {
                // TODO: Oracle Types to CLR Types
                case "INTERVAL YEAR TO MONTH":
                    return isNullable ? typeof(long?) : typeof(long);

                case "BFILE":
                case "BLOB":
                case "LONG RAW":
                case "RAW":
                    return typeof(byte[]);

                case "bit":
                    return isNullable ? typeof(bool?) : typeof(bool);

                case "CHAR":
                case "CLOB":
                case "LONG":
                case "NCHAR":
                case "NCLOB":
                case "REF":
                case "ROWID":
                case "UROWID":
                case "VARCHAR2":
                case "XMLType":
                    return typeof(string);

                case "DATE":
                case "TIMESTAMP":
                case "TIMESTAMP WITH LOCAL TIME ZONE":
                case "TIMESTAMP WITH TIME ZONE":
                    return isNullable ? typeof(DateTime?) : typeof(DateTime);

                case "INTERVAL DAY TO SECOND":
                    return isNullable ? typeof(TimeSpan?) : typeof(TimeSpan);

                case "BINARY_DOUBLE":
                case "BINARY_FLOAT":
                case "BINARY_INTEGER":
                case "NUMBER":
                case "NVARCHAR2":
                case "PLS_INTEGER":
                    return isNullable ? typeof(decimal?) : typeof(decimal);

                default:
                    return typeof(object);
            }
        }
    }
}
