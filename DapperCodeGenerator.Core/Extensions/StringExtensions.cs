using System;
using System.Linq;

namespace DapperCodeGenerator.Core.Extensions
{
    public static class StringExtensions
    {
        public static string Repeat(this string value, int quantity)
        {
            return new System.Text.StringBuilder().Insert(0, value, quantity).ToString();
        }

        public static string CapitalizeFirstLetter(this string input) =>
            string.IsNullOrEmpty(input)
                ? ""
                : input.First().ToString().ToUpper() + input[1..];

        public static string TrimLastCharacters(this string str) => string.IsNullOrEmpty(str) ? str : str.TrimEnd(str[^1]);

        public static string RemoveFirstCharacter(this string str) => string.IsNullOrEmpty(str) ? str : str[1..];

        public static string RemoveLastCharacter(this string str) => string.IsNullOrEmpty(str) ? str : str[..^1];

        public static string RemovePluralization(this string input) =>
            !string.IsNullOrEmpty(input) && input.EndsWith("s", StringComparison.InvariantCultureIgnoreCase) ? input.RemoveLastCharacter() : input;

        public static string ToPascalCase(this string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                return string.Empty;
            }

            return string.Join("", input.Split('_').Select(word =>
                string.IsNullOrEmpty(word) ? string.Empty :
                char.ToUpper(word[0]) + (word.Length > 1 ? word[1..].ToLower() : string.Empty)));
        }
    }
}
