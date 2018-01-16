using System.Collections.Generic;
using DapperCodeGenerator.Core.Enumerations;

namespace DapperCodeGenerator.Core.Models
{
    public class Database
	{
	    public DbConnectionTypes ConnectionType { get; set; }
        public string DatabaseName { get; set; }
        
        public List<DatabaseTable> Tables { get; set; } = new List<DatabaseTable>();
    }
}
