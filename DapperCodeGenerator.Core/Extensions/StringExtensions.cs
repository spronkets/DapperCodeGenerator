using System;
using System.Collections.Generic;
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

        public static string RemovePluralization(this string input)
        {
            if (string.IsNullOrEmpty(input))
                return input;

            // Handle irregular plurals (plural → singular)
            var irregularPlurals = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
            {
                {"People", "Person"},
                {"Children", "Child"},
                {"Men", "Man"},
                {"Women", "Woman"},
                {"Feet", "Foot"},
                {"Teeth", "Tooth"},
                {"Mice", "Mouse"},
                {"Geese", "Goose"}
            };

            if (irregularPlurals.TryGetValue(input, out string singular))
                return singular;

            // Handle words ending in -ies (Categories → Category)
            if (input.EndsWith("ies", StringComparison.InvariantCultureIgnoreCase) && input.Length > 3)
                return input[..^3] + "y";

            // Handle words ending in -es (Boxes → Box, Classes → Class)
            if (input.EndsWith("es", StringComparison.InvariantCultureIgnoreCase) && input.Length > 2)
            {
                var withoutEs = input[..^2];
                // Don't remove -es from words that would be too short or end in vowels
                if (withoutEs.Length > 1 && !"aeiou".Contains(withoutEs[^1], StringComparison.InvariantCultureIgnoreCase))
                    return withoutEs;
            }

            // Handle simple -s plurals (Users → User)
            if (input.EndsWith("s", StringComparison.InvariantCultureIgnoreCase) && input.Length > 1)
                return input.RemoveLastCharacter();

            return input;
        }

        public static string EnsurePluralization(this string input)
        {
            if (string.IsNullOrEmpty(input))
                return input;

            // Handle irregular plurals (singular → plural)
            var irregularSingulars = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
            {
                {"Person", "People"},
                {"Child", "Children"},
                {"Man", "Men"},
                {"Woman", "Women"},
                {"Foot", "Feet"},
                {"Tooth", "Teeth"},
                {"Mouse", "Mice"},
                {"Goose", "Geese"}
            };

            if (irregularSingulars.TryGetValue(input, out string plural))
                return plural;

            // Already ends in 's', likely already plural
            if (input.EndsWith("s", StringComparison.InvariantCultureIgnoreCase))
                return input;

            // Handle words ending in -y (Category → Categories)
            if (input.EndsWith("y", StringComparison.InvariantCultureIgnoreCase) && input.Length > 1)
            {
                var beforeY = input[^2];
                // If consonant before 'y', change to 'ies'
                if (!"aeiou".Contains(beforeY, StringComparison.InvariantCultureIgnoreCase))
                    return input[..^1] + "ies";
            }

            // Handle words ending in -s, -x, -z, -ch, -sh (Box → Boxes)
            if (input.EndsWith("s", StringComparison.InvariantCultureIgnoreCase) ||
                input.EndsWith("x", StringComparison.InvariantCultureIgnoreCase) ||
                input.EndsWith("z", StringComparison.InvariantCultureIgnoreCase) ||
                input.EndsWith("ch", StringComparison.InvariantCultureIgnoreCase) ||
                input.EndsWith("sh", StringComparison.InvariantCultureIgnoreCase))
                return input + "es";

            // Simple case: add 's'
            return input + "s";
        }

        public static string ToPascalCase(this string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                return string.Empty;
            }

            // If input contains underscores, split and convert each part
            if (input.Contains('_'))
            {
                return string.Join("", input.Split('_').Select(word =>
                    string.IsNullOrEmpty(word) ? string.Empty :
                    char.ToUpper(word[0]) + (word.Length > 1 ? word[1..].ToLower() : string.Empty)));
            }

            // If no underscores, assume it's already PascalCase or needs simple conversion
            return char.ToUpper(input[0]) + (input.Length > 1 ? input[1..] : string.Empty);
        }

        public static string ToCamelCase(this string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                return string.Empty;
            }

            var pascalCase = input.ToPascalCase();
            if (string.IsNullOrEmpty(pascalCase))
            {
                return string.Empty;
            }

            return char.ToLower(pascalCase[0]) + (pascalCase.Length > 1 ? pascalCase[1..] : string.Empty);
        }
    }
}
