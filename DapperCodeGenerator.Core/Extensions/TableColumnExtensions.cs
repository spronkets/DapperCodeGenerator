using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DapperCodeGenerator.Core.Models;

namespace DapperCodeGenerator.Core.Extensions
{
    public static class TableColumnExtensions
    {
        public static string GetColumnNameAsParameter(this DatabaseTableColumn tableColumn)
        {
            return $"{char.ToLowerInvariant(tableColumn.ColumnName[0])}{tableColumn.ColumnName.Substring(1)}";
        }

        public static string GetMethodParameters(this IEnumerable<DatabaseTableColumn> tableColumns, bool parametersAreOptional = false)
        {
            var methodParametersBuilder = new StringBuilder();

            var tableColumnsCount = tableColumns.Count();
            for(var i = 0; i < tableColumnsCount; i++)
            {
                var column = tableColumns.ElementAt(i);
                var columnCodingType = column.Type.GetNameForCoding();

                if (parametersAreOptional && column.Type.IsValueType && !columnCodingType.EndsWith("?"))
                {
                    columnCodingType += "?";
                }

                methodParametersBuilder.Append($"{columnCodingType} {column.GetColumnNameAsParameter()}");
                
                if (i < tableColumnsCount - 1)
                {
                    methodParametersBuilder.Append(", ");
                }
            }

            return methodParametersBuilder.ToString();
        }
        
        public static string GetSqlWhereClauses(this IEnumerable<DatabaseTableColumn> tableColumns, string dbParameterCharacter = "@", bool parametersAreOptional = false)
        {
            var sqlBuilder = new StringBuilder();

            var tableColumnsCount = tableColumns.Count();
            for (var i = 0; i < tableColumnsCount; i++)
            {
                var column = tableColumns.ElementAt(i);
                var columnNameAsParameter = column.GetColumnNameAsParameter();
                
                if (parametersAreOptional)
                {
                    sqlBuilder.Append($"({dbParameterCharacter}{columnNameAsParameter} IS NULL OR {column.ColumnName} = {dbParameterCharacter}{columnNameAsParameter})");
                }
                else
                {
                    sqlBuilder.Append($"{column.ColumnName} = {dbParameterCharacter}{columnNameAsParameter}");
                }
                
                if (i < tableColumnsCount - 1)
                {
                    sqlBuilder.Append(", ");
                }
            }

            return sqlBuilder.ToString();
        }

        public static string GetColumnNames(this IEnumerable<DatabaseTableColumn> tableColumns)
        {
            return string.Join(", ", tableColumns.Select(tc => tc.ColumnName));
        }

        public static string GetSqlInsertValues(this IEnumerable<DatabaseTableColumn> tableColumns, string dbParameterCharacter = "@")
        {
            var sqlBuilder = new StringBuilder();

            var tableColumnsCount = tableColumns.Count();
            for (var i = 0; i < tableColumnsCount; i++)
            {
                var column = tableColumns.ElementAt(i);
                var columnNameAsParameter = column.GetColumnNameAsParameter();

                sqlBuilder.Append($"{dbParameterCharacter}{columnNameAsParameter}");

                if (i < tableColumnsCount - 1)
                {
                    sqlBuilder.Append(", ");
                }
            }

            return sqlBuilder.ToString();
        }

        public static string GetDapperProperties(this IEnumerable<DatabaseTableColumn> tableColumns)
        {
            return string.Join(", ", tableColumns.Select(tc => tc.GetColumnNameAsParameter()));
        }
    }
}
