using System;
using DapperCodeGenerator.Core.Enumerations;

namespace DapperCodeGenerator.Core.Generators
{
    public static class DapperGeneratorFactory
    {
        public static DapperGenerator GetGenerator(DbConnectionTypes connectionType)
        {
            return connectionType switch
            {
                DbConnectionTypes.MsSql => new MsSqlDapperGenerator(),
                DbConnectionTypes.MySql => new MySqlDapperGenerator(),
                DbConnectionTypes.Postgres => new PostgresDapperGenerator(),
                DbConnectionTypes.Oracle => new OracleDapperGenerator(),
                _ => throw new NotSupportedException($"Dapper generator not supported for {connectionType} database type.")
            };
        }
    }
}