using System.Collections.Generic;
using DapperCodeGenerator.Core.Enumerations;
using DapperCodeGenerator.Core.Models;
using DapperCodeGenerator.Core.Providers;

namespace DapperCodeGenerator.Web.Models
{
    public class ApplicationState
    {
        public DbConnectionTypes DbConnectionType { get; set; } = DbConnectionTypes.MsSql;
        public string ConnectionString { get; set; } = "";
        public Provider CurrentProvider { get; set; }
        public List<Database> Databases { get; set; } = new List<Database>();
        public Database SelectedDatabase { get; set; }
    }
}