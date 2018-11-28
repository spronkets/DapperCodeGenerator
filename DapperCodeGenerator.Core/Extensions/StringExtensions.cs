namespace DapperCodeGenerator.Core.Extensions
{
    public static class StringExtensions
    {
        public static string Repeat(this string value, int quantity)
        {
            return new System.Text.StringBuilder().Insert(0, value, quantity).ToString();
        }
    }
}
