using System.Collections.Generic;
using DapperCodeGenerator.Core.Enumerations;
using DapperCodeGenerator.Core.Extensions;

namespace DapperCodeGenerator.Core.Models
{
    public class DatabaseTable
    {
        public DbConnectionTypes ConnectionType { get; set; }
        public string DatabaseName { get; set; }
        public string TableName { get; set; }

        public string DataModelName => $"{TableName.CapitalizeFirstLetter().RemovePluralization()}DataModel";

        public List<DatabaseTableColumn> Columns { get; set; } = new List<DatabaseTableColumn>();
    }
}
