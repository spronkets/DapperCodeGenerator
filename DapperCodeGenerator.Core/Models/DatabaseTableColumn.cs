using System;
using System.Collections.Generic;
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

        public List<string> PrimaryKeys { get; set; } = new List<string>();
        public bool IsPrimaryKey => PrimaryKeys?.Count > 0;

        public List<string> UniqueKeys { get; set; } = new List<string>();
        public bool IsUniqueKey => UniqueKeys?.Count > 0;

        public List<string> ForeignKeys { get; set; } = new List<string>();
        public bool IsForeignKey => ForeignKeys?.Count > 0;
    }
}
