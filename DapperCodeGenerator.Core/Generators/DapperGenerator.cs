using System.Linq;
using System.Text;
using DapperCodeGenerator.Core.Extensions;
using DapperCodeGenerator.Core.Models;

namespace DapperCodeGenerator.Core.Generators
{
    public abstract class DapperGenerator
    {
        protected const string ConnectionStringPlaceholder = "\"<ConnectionString>\"";

        protected abstract void GenerateGetMethods(StringBuilder stringBuilder, DatabaseTable table);
        protected abstract void GenerateFindMethods(StringBuilder stringBuilder, DatabaseTable table);
        protected abstract void GenerateInsertMethods(StringBuilder stringBuilder, DatabaseTable table);
        protected abstract void GenerateUpdateMethods(StringBuilder stringBuilder, DatabaseTable table);
        protected abstract void GenerateDeleteMethods(StringBuilder stringBuilder, DatabaseTable table);
        protected abstract void GenerateMergeMethods(StringBuilder stringBuilder, DatabaseTable table);

        public string GenerateDapperFromDatabase(Database database, string defaultNamespace = "DapperCodeGenerator.Repositories")
        {
            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"namespace {defaultNamespace};\n");

            stringBuilder.AppendLine($"public class {database.DatabaseName.ToPascalCase()}Repository");
            stringBuilder.AppendLine("{");

            for (var i = 0; i < database.Tables.Count; i++)
            {
                stringBuilder.Append(GenerateDapperFromTable(database.Tables[i]));

                if (i < database.Tables.Count - 1)
                {
                    stringBuilder.AppendLine();
                }
            }

            stringBuilder.AppendLine("}");
            stringBuilder.AppendLine();

            return stringBuilder.ToString();
        }

        public string GenerateDapperFromTable(DatabaseTable table)
        {
            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"{PadBy(1)}#region {table.TableName}");
            stringBuilder.AppendLine();

            GenerateGetMethods(stringBuilder, table);

            stringBuilder.AppendLine();
            GenerateFindMethods(stringBuilder, table);

            stringBuilder.AppendLine();
            GenerateInsertMethods(stringBuilder, table);

            stringBuilder.AppendLine();
            GenerateUpdateMethods(stringBuilder, table);

            stringBuilder.AppendLine();
            GenerateDeleteMethods(stringBuilder, table);

            stringBuilder.AppendLine();
            GenerateMergeMethods(stringBuilder, table);

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"{PadBy(1)}#endregion {table.TableName}");

            return stringBuilder.ToString();
        }

        protected static string PadBy(int quantity, bool useSpaces = true, int spacesMultiplier = 4)
        {
            return (useSpaces ? " ".Repeat(spacesMultiplier) : "\t").Repeat(quantity);
        }
    }
}