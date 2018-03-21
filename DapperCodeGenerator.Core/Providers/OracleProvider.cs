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
		private readonly OracleConnectionStringBuilder connectionStringBuilder;

		public OracleProvider(string connectionString)
			: base(connectionString)
		{
			connectionStringBuilder = new OracleConnectionStringBuilder(connectionString);
		}

		protected override IEnumerable<Database> GetDatabases()
		{
			return new List<Database> {
				new Database
				{
					ConnectionType = DbConnectionTypes.Oracle,
					DatabaseName = connectionStringBuilder.UserID.ToUpper()
				}
			};
		}

		protected override IEnumerable<DatabaseTable> GetDatabaseTables(string databaseName)
		{
			DataTable selectedDatabaseTables = null;
			try
			{
				using (var db = new OracleConnection($"{connectionStringBuilder}"))
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
					var ownerName = tableRow.ItemArray[0].ToString();
					var tableName = tableRow.ItemArray[1].ToString();
					var typeName = tableRow.ItemArray[2].ToString();
					if (ownerName == databaseName && typeName == "User")
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
			var columns = new List<DatabaseTableColumn>();

			try
			{
				using (var db = new OracleConnection($"{connectionStringBuilder}"))
				{
					using (var cmd = db.CreateCommand())
					{
						db.Open();
						cmd.BindByName = true;

						cmd.CommandText = "select * from user_tab_columns";

						OracleDataReader reader = cmd.ExecuteReader();
						while (reader.Read())
						{
							var columnName = reader["COLUMN_NAME"].ToString();
							var dataType = reader["DATA_TYPE"].ToString();
							var maxLengthStr = reader["DATA_LENGTH"].ToString();
							int.TryParse(maxLengthStr, out var maxLength);
							var isNullable = reader["NULLABLE"].ToString() == "Y";
							var type = GetClrType(dataType, isNullable);
							
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
							columns.Add(column);
						}

						reader.Dispose();
					}
					db.Close();
				}
			}
			catch (Exception exc)
			{
				Console.Error.WriteLine(exc.Message, exc);
			}

			// TODO: Primary Index, Identity, Foreign Key, etc

			return columns;
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
