using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DapperCodeGenerator.Core.Models;

namespace DapperCodeGenerator.Core.Extensions
{
    public static class TableColumnExtensions
    {
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

                methodParametersBuilder.Append($"{columnCodingType} {column.ColumnName.ToCamelCase()}");
                
                if (i < tableColumnsCount - 1)
                {
                    methodParametersBuilder.Append(", ");
                }
            }

            return methodParametersBuilder.ToString();
        }
        

        public static string GetDapperProperties(this IEnumerable<DatabaseTableColumn> tableColumns)
        {
            return string.Join(", ", tableColumns.Select(tc => tc.ColumnName.ToCamelCase()));
        }
    }
}
