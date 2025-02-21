using DapperCodeGenerator.Core.Enumerations;
using DapperCodeGenerator.Core.Providers;
using DapperCodeGenerator.Web.Models;
using Microsoft.AspNetCore.Mvc;

namespace DapperCodeGenerator.Web.Controllers
{
    public class DatabasesController(ApplicationState state) : Controller
    {
        [HttpGet]
        public ActionResult Index()
        {
            UpdateState(DbConnectionTypes.MsSql);

            return View(state);
        }

        [HttpGet]
        public ActionResult SelectConnectionType(DbConnectionTypes connectionType)
        {
            UpdateState(connectionType);

            return View("Index", state);
        }

        [HttpGet]
        public ActionResult Refresh(DbConnectionTypes connectionType, string connectionString)
        {
            UpdateState(connectionType, connectionString);

            state.Databases = state.CurrentProvider?.RefreshDatabases();

            return View("Index", state);
        }

        [HttpGet]
        public ActionResult SelectDatabase(string databaseName)
        {
            state.SelectedDatabase = state.CurrentProvider?.SelectDatabase(state.Databases, databaseName);

            return View("Index", state);
        }

        private void UpdateState(DbConnectionTypes connectionType, string connectionString = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                connectionString = GetDefaultConnectionString(connectionType);
            }

            state.DbConnectionType = connectionType;
            state.ConnectionString = connectionString;
            state.CurrentProvider = GetProvider(connectionType, connectionString);
            state.Databases = null;
            state.SelectedDatabase = null;
        }

        private static string GetDefaultConnectionString(DbConnectionTypes connectionType)
        {
            return connectionType switch
            {
                DbConnectionTypes.MsSql => "Data Source=localhost;Integrated Security=True;TrustServerCertificate=True;",
                DbConnectionTypes.MySql => "Server=127.0.0.1;Port=3306;User Id=root;Password=mysql;",
                DbConnectionTypes.Postgres => "Server=localhost;Port=5432;User Id=postgres;Password=postgres;",
                DbConnectionTypes.Oracle => "Data Source=127.0.0.1:1521/xe;User Id=oracle;Password=oracle;",
                _ => null
            };
        }

        private static Provider GetProvider(DbConnectionTypes connectionType, string connectionString)
        {
            Provider provider;
            switch (connectionType)
            {
                case DbConnectionTypes.MsSql:
                    provider = new MsSqlProvider(connectionString);
                    break;
                case DbConnectionTypes.MySql:
                    provider = new MySqlProvider(connectionString);
                    break;
                case DbConnectionTypes.Postgres:
                    provider = new PostgresProvider(connectionString);
                    break;
                case DbConnectionTypes.Oracle:
                    provider = new OracleProvider(connectionString);
                    break;
                default:
                    return null;
            }

            return provider;
        }
    }
}