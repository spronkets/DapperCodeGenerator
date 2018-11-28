using System;
using System.Collections.Generic;
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
			var tables = new List<DatabaseTable>();

			try
			{
				using (var db = new OracleConnection($"{connectionStringBuilder}"))
				{
					using (var cmd = db.CreateCommand())
					{
						db.Open();
						cmd.BindByName = true;

						cmd.CommandText = "SELECT * FROM USER_TABLES";
						OracleDataReader reader = cmd.ExecuteReader();
						while (reader.Read())
						{
							var tableName = reader["TABLE_NAME"].ToString();

							var table = new DatabaseTable
							{
								ConnectionType = DbConnectionTypes.Oracle,
								DatabaseName = databaseName,
								TableName = tableName
							};
							tables.Add(table);
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

			return tables;
		}

		protected override IEnumerable<DatabaseTableColumn> GetDatabaseTableColumns(string databaseName, string tableName)
		{
			var columns = new List<DatabaseTableColumn>();

			try
			{
				using (var db = new OracleConnection($"{connectionStringBuilder}"))
				{
					db.Open();

					using (var cmd = db.CreateCommand())
					{
						cmd.BindByName = true;

						cmd.CommandText = $"SELECT * FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '{tableName}'";
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
						
						cmd.CommandText = $"SELECT COLUMN_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE FROM USER_CONSTRAINTS NATURAL JOIN USER_CONS_COLUMNS WHERE TABLE_NAME = '{tableName}'";
						reader = cmd.ExecuteReader();
						while (reader.Read())
						{
							var columnName = reader["COLUMN_NAME"].ToString();

							var column = columns.SingleOrDefault(c => c.TableName == tableName && c.ColumnName == columnName);
							if (column != null)
							{
								var constraintName = reader["CONSTRAINT_NAME"].ToString();
								var constraintType = reader["CONSTRAINT_TYPE"].ToString();

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

						reader.Dispose();
					}
					db.Close();
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
			switch (dbTypeName)
			{
				// TODO: Oracle Types to CLR Types
				case "INTERVAL YEAR TO MONTH":
					return isNullable ? typeof(long?) : typeof(long);

				case "BFILE":
				case "BLOB":
				case "LONG RAW":
					return typeof(byte[]);

				case "RAW":
					return typeof(Guid);

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
				case "NVARCHAR2":
				case "XMLType":
					return typeof(string);

				case "DATE":
				case "TIMESTAMP":
				case "TIMESTAMP(4)":
					return isNullable ? typeof(DateTime?) : typeof(DateTime);
					
				case "BINARY_DOUBLE":
				case "BINARY_FLOAT":
				case "BINARY_INTEGER":
				case "NUMBER":
				case "PLS_INTEGER":
					return isNullable ? typeof(decimal?) : typeof(decimal);

				default:
					return typeof(object);
			}
		}
	}
}
