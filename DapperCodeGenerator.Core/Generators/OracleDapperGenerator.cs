using System.Linq;
using System.Text;
using DapperCodeGenerator.Core.Extensions;
using DapperCodeGenerator.Core.Models;

namespace DapperCodeGenerator.Core.Generators
{
    public class OracleDapperGenerator : DapperGenerator
    {
        protected override void GenerateGetMethods(StringBuilder stringBuilder, DatabaseTable table)
        {
            var primaryKeyColumns = table.Columns.Where(tc => tc.IsPrimaryKey).ToList();
            if (primaryKeyColumns.Count > 0)
            {
                var methodParameters = primaryKeyColumns.GetMethodParameters();
                var sqlWhereClauses = primaryKeyColumns.GetSqlWhereClauses(":");
                var dapperProperties = primaryKeyColumns.GetDapperProperties();

                stringBuilder.AppendLine($"{PadBy(1)}public async Task<{table.DataModelName}> Get{table.TableName.ToPascalCase().RemovePluralization()}ByIdAsync({methodParameters})");
                stringBuilder.AppendLine($"{PadBy(1)}{{");
                stringBuilder.AppendLine($"{PadBy(2)}using var db = new OracleConnection({ConnectionStringPlaceholder});");
                stringBuilder.AppendLine($"{PadBy(2)}const string getByIdQuery = \"SELECT * FROM {table.TableName} WHERE {sqlWhereClauses}\";");
                stringBuilder.AppendLine($"{PadBy(2)}return await db.QuerySingleAsync<{table.DataModelName}>(getByIdQuery, new {{ {dapperProperties} }});");
                stringBuilder.AppendLine($"{PadBy(1)}}}");
            }
            else
            {
                stringBuilder.AppendLine($"{PadBy(1)}// INFO: There are no primary keys for the Dapper code generation tool to generate get method(s).");
            }
        }

        protected override void GenerateFindMethods(StringBuilder stringBuilder, DatabaseTable table)
        {
            stringBuilder.AppendLine($"{PadBy(1)}public async Task<IEnumerable<{table.DataModelName}>> GetAll{table.TableName.ToPascalCase().EnsurePluralization()}Async()");
            stringBuilder.AppendLine($"{PadBy(1)}{{");
            stringBuilder.AppendLine($"{PadBy(2)}using var db = new OracleConnection({ConnectionStringPlaceholder});");
            stringBuilder.AppendLine($"{PadBy(2)}const string getAllQuery = \"SELECT * FROM {table.TableName}\";");
            stringBuilder.AppendLine($"{PadBy(2)}var results = await db.QueryAsync<{table.DataModelName}>(getAllQuery);");
            stringBuilder.AppendLine($"{PadBy(2)}return results;");
            stringBuilder.AppendLine($"{PadBy(1)}}}");

            stringBuilder.AppendLine();

            var methodParameters = table.Columns.GetMethodParameters(parametersAreOptional: true);
            var sqlWhereClauses = table.Columns.GetSqlWhereClauses(":", parametersAreOptional: true);
            var dapperProperties = table.Columns.GetDapperProperties();

            stringBuilder.AppendLine($"{PadBy(1)}public async Task<IEnumerable<{table.DataModelName}>> Find{table.TableName.ToPascalCase().EnsurePluralization()}Async({methodParameters})");
            stringBuilder.AppendLine($"{PadBy(1)}{{");
            stringBuilder.AppendLine($"{PadBy(2)}using var db = new OracleConnection({ConnectionStringPlaceholder});");
            stringBuilder.AppendLine($"{PadBy(2)}const string findQuery = \"SELECT * FROM {table.TableName} WHERE {sqlWhereClauses}\";");
            stringBuilder.AppendLine($"{PadBy(2)}var results = await db.QueryAsync<{table.DataModelName}>(findQuery, new {{ {dapperProperties} }});");
            stringBuilder.AppendLine($"{PadBy(2)}return results;");
            stringBuilder.AppendLine($"{PadBy(1)}}}");

            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"{PadBy(1)}public async Task<IEnumerable<{table.DataModelName}>> Find{table.TableName.ToPascalCase().EnsurePluralization()}ByAnyAsync({methodParameters})");
            stringBuilder.AppendLine($"{PadBy(1)}{{");
            stringBuilder.AppendLine($"{PadBy(2)}using var db = new OracleConnection({ConnectionStringPlaceholder});");
            stringBuilder.AppendLine($"{PadBy(2)}const string findByAnyQuery = \"SELECT * FROM {table.TableName} WHERE {sqlWhereClauses.Replace(" AND ", " OR ").Replace("IS NULL OR", "IS NOT NULL AND")}\";");
            stringBuilder.AppendLine($"{PadBy(2)}var results = await db.QueryAsync<{table.DataModelName}>(findByAnyQuery, new {{ {dapperProperties} }});");
            stringBuilder.AppendLine($"{PadBy(2)}return results;");
            stringBuilder.AppendLine($"{PadBy(1)}}}");
        }

        protected override void GenerateInsertMethods(StringBuilder stringBuilder, DatabaseTable table)
        {
            var methodParameters = table.Columns.GetMethodParameters();
            var columnNames = table.Columns.GetColumnNames();
            var sqlInsertValues = table.Columns.GetSqlInsertValues(":");
            var dapperParameters = table.Columns.GetDapperProperties();

            stringBuilder.AppendLine($"{PadBy(1)}public async Task<int> Create{table.TableName.ToPascalCase().RemovePluralization()}Async({methodParameters})");
            stringBuilder.AppendLine($"{PadBy(1)}{{");
            stringBuilder.AppendLine($"{PadBy(2)}using var db = new OracleConnection({ConnectionStringPlaceholder});");
            stringBuilder.AppendLine($"{PadBy(2)}const string insertQuery = \"INSERT INTO {table.TableName} ({columnNames}) VALUES ({sqlInsertValues})\";");
            stringBuilder.AppendLine($"{PadBy(2)}var rowsAffected = await db.ExecuteScalarAsync<int>(insertQuery, new {{ {dapperParameters} }});");
            stringBuilder.AppendLine($"{PadBy(2)}return rowsAffected;");
            stringBuilder.AppendLine($"{PadBy(1)}}}");
        }

        protected override void GenerateUpdateMethods(StringBuilder stringBuilder, DatabaseTable table)
        {
            var primaryKeyColumns = table.Columns.Where(tc => tc.IsPrimaryKey).ToList();
            if (primaryKeyColumns.Count > 0)
            {
                var methodParameters = table.Columns.GetMethodParameters();
                var sqlSetClauses = table.Columns.GetSqlWhereClauses(":");
                var sqlWhereClausesForPrimaryKeys = primaryKeyColumns.GetSqlWhereClauses(":");
                var dapperParametersForPrimaryKeys = primaryKeyColumns.GetDapperProperties();

                stringBuilder.AppendLine($"{PadBy(1)}public async Task<int> Update{table.TableName.ToPascalCase().RemovePluralization()}Async({methodParameters})");
                stringBuilder.AppendLine($"{PadBy(1)}{{");
                stringBuilder.AppendLine($"{PadBy(2)}using var db = new OracleConnection({ConnectionStringPlaceholder});");
                stringBuilder.AppendLine($"{PadBy(2)}const string updateQuery = \"UPDATE {table.TableName} SET {sqlSetClauses} WHERE {sqlWhereClausesForPrimaryKeys}\";");
                stringBuilder.AppendLine($"{PadBy(2)}var rowsAffected = await db.ExecuteScalarAsync<int>(updateQuery, new {{ {dapperParametersForPrimaryKeys} }});");
                stringBuilder.AppendLine($"{PadBy(2)}return rowsAffected;");
                stringBuilder.AppendLine($"{PadBy(1)}}}");
            }
            else
            {
                stringBuilder.AppendLine($"{PadBy(1)}// INFO: There are no primary keys for the Dapper code generation tool to generate update method(s).");
            }
        }

        protected override void GenerateDeleteMethods(StringBuilder stringBuilder, DatabaseTable table)
        {
            var primaryKeyColumns = table.Columns.Where(tc => tc.IsPrimaryKey).ToList();
            if (primaryKeyColumns.Count > 0)
            {
                var methodParameters = primaryKeyColumns.GetMethodParameters();
                var sqlWhereClausesForPrimaryKeys = primaryKeyColumns.GetSqlWhereClauses(":");
                var dapperParametersForPrimaryKeys = primaryKeyColumns.GetDapperProperties();

                stringBuilder.AppendLine($"{PadBy(1)}public async Task<int> Delete{table.TableName.ToPascalCase().RemovePluralization()}Async({methodParameters})");
                stringBuilder.AppendLine($"{PadBy(1)}{{");
                stringBuilder.AppendLine($"{PadBy(2)}using var db = new OracleConnection({ConnectionStringPlaceholder});");
                stringBuilder.AppendLine($"{PadBy(2)}const string deleteQuery = \"DELETE FROM {table.TableName} WHERE {sqlWhereClausesForPrimaryKeys}\";");
                stringBuilder.AppendLine($"{PadBy(2)}var rowsAffected = await db.ExecuteScalarAsync<int>(deleteQuery, new {{ {dapperParametersForPrimaryKeys} }});");
                stringBuilder.AppendLine($"{PadBy(2)}return rowsAffected;");
                stringBuilder.AppendLine($"{PadBy(1)}}}");
            }
            else
            {
                stringBuilder.AppendLine($"{PadBy(1)}// INFO: There are no primary keys for the Dapper code generation tool to generate delete method(s).");
            }
        }

        protected override void GenerateMergeMethods(StringBuilder stringBuilder, DatabaseTable table)
        {
            var primaryKeyColumns = table.Columns.Where(tc => tc.IsPrimaryKey).ToList();
            if (primaryKeyColumns.Count > 0)
            {
                var methodParameters = table.Columns.GetMethodParameters();
                var allColumns = table.Columns.ToList();
                var nonPrimaryColumns = allColumns.Where(c => !c.IsPrimaryKey).ToList();
                var dapperProperties = table.Columns.GetDapperProperties();

                var sourceColumns = string.Join(", ", allColumns.Select(c => $":{c.ColumnName.ToCamelCase()} AS {c.ColumnName}"));
                var mergeMatchConditions = string.Join(" AND ", primaryKeyColumns.Select(c => $"target.{c.ColumnName} = source.{c.ColumnName}"));
                var updateSetClauses = string.Join(", ", nonPrimaryColumns.Select(c => $"target.{c.ColumnName} = source.{c.ColumnName}"));
                var insertColumns = string.Join(", ", allColumns.Select(c => c.ColumnName));
                var insertValues = string.Join(", ", allColumns.Select(c => $"source.{c.ColumnName}"));

                stringBuilder.AppendLine($"{PadBy(1)}public async Task<int> Upsert{table.TableName.ToPascalCase().RemovePluralization()}Async({methodParameters})");
                stringBuilder.AppendLine($"{PadBy(1)}{{");
                stringBuilder.AppendLine($"{PadBy(2)}using var db = new OracleConnection({ConnectionStringPlaceholder});");
                stringBuilder.AppendLine($"{PadBy(2)}const string upsertQuery = @\"");
                stringBuilder.AppendLine($"{PadBy(3)}MERGE INTO {table.TableName} target");
                stringBuilder.AppendLine($"{PadBy(3)}USING (SELECT {sourceColumns} FROM DUAL) source");
                stringBuilder.AppendLine($"{PadBy(3)}ON ({mergeMatchConditions})");

                if (nonPrimaryColumns.Any())
                {
                    stringBuilder.AppendLine($"{PadBy(3)}WHEN MATCHED THEN");
                    stringBuilder.AppendLine($"{PadBy(4)}UPDATE SET {updateSetClauses}");
                }

                stringBuilder.AppendLine($"{PadBy(3)}WHEN NOT MATCHED THEN");
                stringBuilder.AppendLine($"{PadBy(4)}INSERT ({insertColumns}) VALUES ({insertValues})\";");
                stringBuilder.AppendLine($"{PadBy(2)}var rowsAffected = await db.ExecuteAsync(upsertQuery, new {{ {dapperProperties} }});");
                stringBuilder.AppendLine($"{PadBy(2)}return rowsAffected;");
                stringBuilder.AppendLine($"{PadBy(1)}}}");
            }
            else
            {
                stringBuilder.AppendLine($"{PadBy(1)}// INFO: There are no primary keys for the Dapper code generation tool to generate merge method(s).");
            }
        }
    }
}