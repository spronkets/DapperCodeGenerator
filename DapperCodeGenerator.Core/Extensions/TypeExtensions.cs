using System;
using System.CodeDom;
using Microsoft.CSharp;

namespace DapperCodeGenerator.Core.Extensions
{
    public static class TypeExtensions
    {
        public static string GetNameForCoding(this Type type)
        {
            if (type.FullName != null && !type.FullName.StartsWith("System"))
            {
                return type.Name;
            }

            var compiler = new CSharpCodeProvider();
            var typeRef = new CodeTypeReference(type);
            var typeOutput = compiler.GetTypeOutput(typeRef);
            typeOutput = typeOutput.Replace("System.", "");
            if (typeOutput.Contains("Nullable<"))
            {
                typeOutput = typeOutput.Replace("Nullable", "").Replace(">", "").Replace("<", "") + "?";
            }

            return typeOutput;
        }
    }
}
