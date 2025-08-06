using Markdig;

namespace DapperCodeGenerator.WebBlazor.Extensions;

public static class MarkdownExtensions
{
    public static string ToHtml(this string markdown)
    {
        return Markdown.ToHtml(markdown ?? "");
    }
}
