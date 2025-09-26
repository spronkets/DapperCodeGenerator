using System.Collections.Generic;
using System.Linq;
using System.Text;
using DapperCodeGenerator.Core.Extensions;
using DapperCodeGenerator.Core.Models;

namespace DapperCodeGenerator.Core.Generators
{
    public class OracleDapperGenerator : DapperGenerator
    {
        protected override string GetParameterMarker() => ":";

        protected override string QuoteIdentifier(string identifier) => $"\\\"{identifier}\\\"";

        protected override string BuildWhereClause(IEnumerable<DatabaseTableColumn> columns, bool parametersAreOptional = false)
        {
            var clauses = columns.Select(column =>
            {
                var paramName = $"{GetParameterMarker()}{column.ColumnName.ToCamelCase()}";
                var columnName = QuoteIdentifier(column.ColumnName);

                return parametersAreOptional
                    ? $"({paramName} IS NULL OR {columnName} = {paramName})"
                    : $"{columnName} = {paramName}";
            });

            return string.Join(" AND ", clauses);
        }

        protected override string BuildSetClause(IEnumerable<DatabaseTableColumn> columns)
        {
            var clauses = columns.Select(column =>
            {
                var paramName = $"{GetParameterMarker()}{column.ColumnName.ToCamelCase()}";
                var columnName = QuoteIdentifier(column.ColumnName);
                return $"{columnName} = {paramName}";
            });

            return string.Join(", ", clauses);
        }

        protected override string FormatColumnList(IEnumerable<DatabaseTableColumn> columns)
        {
            return string.Join(", ", columns.Select(column => QuoteIdentifier(column.ColumnName)));
        }

        protected override string BuildParameterList(IEnumerable<DatabaseTableColumn> columns)
        {
            return string.Join(", ", columns.Select(column => $"{GetParameterMarker()}{column.ColumnName.ToCamelCase()}"));
        }
        protected override void GenerateGetMethods(StringBuilder stringBuilder, DatabaseTable table)
        {
            var primaryKeyColumns = table.Columns.Where(tc => tc.IsPrimaryKey).ToList();
            if (primaryKeyColumns.Count > 0)
            {
                var methodParameters = primaryKeyColumns.GetMethodParameters();
                var sqlWhereClauses = BuildWhereClause(primaryKeyColumns);
                var dapperProperties = primaryKeyColumns.GetDapperProperties();

                stringBuilder.AppendLine($"{PadBy(1)}public async Task<{table.DataModelName}> Get{table.TableName.ToPascalCase().RemovePluralization()}ByIdAsync({methodParameters})");
                stringBuilder.AppendLine($"{PadBy(1)}{{");
                stringBuilder.AppendLine($"{PadBy(2)}using var db = new OracleConnection({ConnectionStringPlaceholder});");
                stringBuilder.AppendLine($"{PadBy(2)}const string getByIdQuery = \"SELECT * FROM {QuoteIdentifier(table.TableName)} WHERE {sqlWhereClauses}\";");
                stringBuilder.AppendLine($"{PadBy(2)}var result = await db.QuerySingleAsync<{table.DataModelName}>(getByIdQuery, new {{ {dapperProperties} }});");
                stringBuilder.AppendLine($"{PadBy(2)}return result;");
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
            stringBuilder.AppendLine($"{PadBy(2)}const string getAllQuery = \"SELECT * FROM {QuoteIdentifier(table.TableName)}\";");
            stringBuilder.AppendLine($"{PadBy(2)}var results = await db.QueryAsync<{table.DataModelName}>(getAllQuery);");
            stringBuilder.AppendLine($"{PadBy(2)}return results;");
            stringBuilder.AppendLine($"{PadBy(1)}}}");

            stringBuilder.AppendLine();

            var methodParameters = table.Columns.GetMethodParameters(parametersAreOptional: true);
            var sqlWhereClauses = BuildWhereClause(table.Columns, parametersAreOptional: true);
            var dapperProperties = table.Columns.GetDapperProperties();

            stringBuilder.AppendLine($"{PadBy(1)}public async Task<IEnumerable<{table.DataModelName}>> Find{table.TableName.ToPascalCase().EnsurePluralization()}Async({methodParameters})");
            stringBuilder.AppendLine($"{PadBy(1)}{{");
            stringBuilder.AppendLine($"{PadBy(2)}using var db = new OracleConnection({ConnectionStringPlaceholder});");
            stringBuilder.AppendLine($"{PadBy(2)}const string findQuery = \"SELECT * FROM {QuoteIdentifier(table.TableName)} WHERE {sqlWhereClauses}\";");
            stringBuilder.AppendLine($"{PadBy(2)}var results = await db.QueryAsync<{table.DataModelName}>(findQuery, new {{ {dapperProperties} }});");
            stringBuilder.AppendLine($"{PadBy(2)}return results;");
            stringBuilder.AppendLine($"{PadBy(1)}}}");

            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"{PadBy(1)}public async Task<IEnumerable<{table.DataModelName}>> Find{table.TableName.ToPascalCase().EnsurePluralization()}ByAnyAsync({methodParameters})");
            stringBuilder.AppendLine($"{PadBy(1)}{{");
            stringBuilder.AppendLine($"{PadBy(2)}using var db = new OracleConnection({ConnectionStringPlaceholder});");
            stringBuilder.AppendLine($"{PadBy(2)}const string findByAnyQuery = \"SELECT * FROM {QuoteIdentifier(table.TableName)} WHERE {sqlWhereClauses.Replace(" AND ", " OR ").Replace("IS NULL OR", "IS NOT NULL AND")}\";");
            stringBuilder.AppendLine($"{PadBy(2)}var results = await db.QueryAsync<{table.DataModelName}>(findByAnyQuery, new {{ {dapperProperties} }});");
            stringBuilder.AppendLine($"{PadBy(2)}return results;");
            stringBuilder.AppendLine($"{PadBy(1)}}}");
        }

        protected override void GenerateInsertMethods(StringBuilder stringBuilder, DatabaseTable table)
        {
            var methodParameters = table.Columns.GetMethodParameters();
            var columnNames = FormatColumnList(table.Columns);
            var sqlInsertValues = BuildParameterList(table.Columns);
            var dapperParameters = table.Columns.GetDapperProperties();

            stringBuilder.AppendLine($"{PadBy(1)}public async Task<int> Create{table.TableName.ToPascalCase().RemovePluralization()}Async({methodParameters})");
            stringBuilder.AppendLine($"{PadBy(1)}{{");
            stringBuilder.AppendLine($"{PadBy(2)}using var db = new OracleConnection({ConnectionStringPlaceholder});");
            stringBuilder.AppendLine($"{PadBy(2)}const string insertQuery = \"INSERT INTO {QuoteIdentifier(table.TableName)} ({columnNames}) VALUES ({sqlInsertValues})\";");
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
                var sqlSetClauses = BuildSetClause(table.Columns.Where(c => !c.IsPrimaryKey));
                var sqlWhereClausesForPrimaryKeys = BuildWhereClause(primaryKeyColumns);
                var dapperParameters = table.Columns.GetDapperProperties();

                stringBuilder.AppendLine($"{PadBy(1)}public async Task<int> Update{table.TableName.ToPascalCase().RemovePluralization()}Async({methodParameters})");
                stringBuilder.AppendLine($"{PadBy(1)}{{");
                stringBuilder.AppendLine($"{PadBy(2)}using var db = new OracleConnection({ConnectionStringPlaceholder});");
                stringBuilder.AppendLine($"{PadBy(2)}const string updateQuery = \"UPDATE {QuoteIdentifier(table.TableName)} SET {sqlSetClauses} WHERE {sqlWhereClausesForPrimaryKeys}\";");
                stringBuilder.AppendLine($"{PadBy(2)}var rowsAffected = await db.ExecuteAsync(updateQuery, new {{ {dapperParameters} }});");
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
                var sqlWhereClausesForPrimaryKeys = BuildWhereClause(primaryKeyColumns);
                var dapperParametersForPrimaryKeys = primaryKeyColumns.GetDapperProperties();

                stringBuilder.AppendLine($"{PadBy(1)}public async Task<int> Delete{table.TableName.ToPascalCase().RemovePluralization()}Async({methodParameters})");
                stringBuilder.AppendLine($"{PadBy(1)}{{");
                stringBuilder.AppendLine($"{PadBy(2)}using var db = new OracleConnection({ConnectionStringPlaceholder});");
                stringBuilder.AppendLine($"{PadBy(2)}const string deleteQuery = \"DELETE FROM {QuoteIdentifier(table.TableName)} WHERE {sqlWhereClausesForPrimaryKeys}\";");
                stringBuilder.AppendLine($"{PadBy(2)}var rowsAffected = await db.ExecuteAsync(deleteQuery, new {{ {dapperParametersForPrimaryKeys} }});");
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

                var sourceColumns = string.Join(", ", allColumns.Select(c => $":{c.ColumnName.ToCamelCase()} AS {QuoteIdentifier(c.ColumnName)}"));
                var mergeMatchConditions = string.Join(" AND ", primaryKeyColumns.Select(c => $"target.{QuoteIdentifier(c.ColumnName)} = source.{QuoteIdentifier(c.ColumnName)}"));
                var updateSetClauses = string.Join(", ", nonPrimaryColumns.Select(c => $"target.{QuoteIdentifier(c.ColumnName)} = source.{QuoteIdentifier(c.ColumnName)}"));
                var insertColumns = string.Join(", ", allColumns.Select(c => QuoteIdentifier(c.ColumnName)));
                var insertValues = string.Join(", ", allColumns.Select(c => $"source.{QuoteIdentifier(c.ColumnName)}"));

                stringBuilder.AppendLine($"{PadBy(1)}public async Task<int> Upsert{table.TableName.ToPascalCase().RemovePluralization()}Async({methodParameters})");
                stringBuilder.AppendLine($"{PadBy(1)}{{");
                stringBuilder.AppendLine($"{PadBy(2)}using var db = new OracleConnection({ConnectionStringPlaceholder});");
                stringBuilder.AppendLine($"{PadBy(2)}const string upsertQuery = @\"");
                stringBuilder.AppendLine($"{PadBy(3)}MERGE INTO {QuoteIdentifier(table.TableName)} target");
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