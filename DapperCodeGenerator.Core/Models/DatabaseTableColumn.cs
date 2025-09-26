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

        public int? NumericPrecision { get; set; }
        public int? NumericScale { get; set; }
        public string DefaultValue { get; set; }
        public int OrdinalPosition { get; set; }
        public bool IsNullable { get; set; }
        public bool? IsAutoIncrement { get; set; }
        public bool? IsComputed { get; set; }
        public bool? IsGenerated { get; set; }

        public List<string> PrimaryKeys { get; set; } = [];
        public bool IsPrimaryKey => PrimaryKeys?.Count > 0;

        public List<string> UniqueKeys { get; set; } = [];
        public bool IsUniqueKey => UniqueKeys?.Count > 0;

        public List<string> ForeignKeys { get; set; } = [];
        public bool IsForeignKey => ForeignKeys?.Count > 0;
    }
}
