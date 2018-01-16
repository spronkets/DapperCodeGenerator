using System;
using DapperCodeGenerator.Core.Enumerations;

namespace DapperCodeGenerator.Core.Models
{
    public class DatabaseTableColumn
    {
        public DbConnectionTypes ConnectionType { get; set; }
        public string DatabaseName { get; set; }
        public string TableName { get; set; }
        public string ColumnName { get; set; }

        public string DataType { get; set; }
        public Type Type { get; set; }
        public string TypeNamespace { get; set; }

        public int MaxLength { get; set; }
        public bool PrimaryKey { get; set; }
    }
}
