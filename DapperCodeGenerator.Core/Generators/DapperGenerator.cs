using System.Text;
using DapperCodeGenerator.Core.Models;

namespace DapperCodeGenerator.Core.Generators
{
    public static class DapperGenerator
    {
        public static string GenerateDapperFromDatabase(Database database, string dataNamespace = "")
        {
            var stringBuilder = new StringBuilder();

            if (string.IsNullOrEmpty(dataNamespace))
            {
                dataNamespace = "DapperCodeGenerator.Repositories";
            }

            stringBuilder.AppendLine($"namespace {dataNamespace}");
            stringBuilder.AppendLine("{");

            stringBuilder.AppendLine($"\tpublic class {database.DatabaseName}Repository");
            stringBuilder.AppendLine("\t{");

            for (var i = 0; i < database.Tables.Count; i++)
            {
                stringBuilder.Append(GenerateDapperFromTable(database.Tables[i]));

                if (i < database.Tables.Count - 1)
                {
                    stringBuilder.AppendLine();
                }
            }

            stringBuilder.AppendLine("\t}");

            stringBuilder.AppendLine("}");

            stringBuilder.AppendLine();

            return stringBuilder.ToString();
        }

        // TODO: MsSql/Dapper differences
        public static string GenerateDapperFromTable(DatabaseTable table)
        {
            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"\t\t#region {table.TableName}");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine("\t\t// Alternative: https://github.com/MoonStorm/Dapper.FastCRUD");
            stringBuilder.AppendLine("\t\t// Alternative: https://github.com/ericdc1/Dapper.SimpleCRUD");

            stringBuilder.AppendLine();

            // TODO: Generate CRUD
            stringBuilder.AppendLine($"\t\t// TODO: Generate {table.TableName} CRUD");
            stringBuilder.AppendLine("\t\t// Sorry, this isn't done yet. Consider using one of the alternatives above and/or write your own for now.");

            GenerateDapperGetMethodsFromTable(stringBuilder, table);

            //stringBuilder.AppendLine();

            GenerateDapperInsertMethodsFromTable(stringBuilder, table);

            //stringBuilder.AppendLine();

            GenerateDapperUpdateMethodsFromTable(stringBuilder, table);

            //stringBuilder.AppendLine();

            GenerateDapperMergeMethodsFromTable(stringBuilder, table);

            //stringBuilder.AppendLine();

            GenerateDapperDeleteMethodsFromTable(stringBuilder, table);

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\t\t#endregion {table.TableName}");

            return stringBuilder.ToString();
        }

        private static void GenerateDapperGetMethodsFromTable(StringBuilder stringBuilder, DatabaseTable table)
        {

        }

        private static void GenerateDapperUpdateMethodsFromTable(StringBuilder stringBuilder, DatabaseTable table)
        {

        }

        private static void GenerateDapperInsertMethodsFromTable(StringBuilder stringBuilder, DatabaseTable table)
        {

        }

        private static void GenerateDapperMergeMethodsFromTable(StringBuilder stringBuilder, DatabaseTable table)
        {

        }

        private static void GenerateDapperDeleteMethodsFromTable(StringBuilder stringBuilder, DatabaseTable table)
        {

        }
    }
}
