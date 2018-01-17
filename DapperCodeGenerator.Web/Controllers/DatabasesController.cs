using DapperCodeGenerator.Core.Enumerations;
using DapperCodeGenerator.Core.Providers;
using DapperCodeGenerator.Web.Models;
using Microsoft.AspNetCore.Mvc;

namespace DapperCodeGenerator.Web.Controllers
{
    public class DatabasesController : Controller
    {
        private readonly ApplicationState state;

        public DatabasesController(ApplicationState state)
        {
            this.state = state;
        }

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

        private string GetDefaultConnectionString(DbConnectionTypes connectionType)
        {
            switch (connectionType)
            {
                case DbConnectionTypes.MsSql:
                    return "Data Source=localhost;Integrated Security=True;";
                case DbConnectionTypes.Postgres:
                    return "Server=localhost;Port=5432;User Id=postgres;Password=postgres;";
                case DbConnectionTypes.Oracle:
                    return "Data Source=127.0.0.1:1521/xe;User Id=oracle;Password=oracle;";
                default:
                    return null;
            }
        }

        private Provider GetProvider(DbConnectionTypes connectionType, string connectionString)
        {
            Provider provider;
            switch (connectionType)
            {
                case DbConnectionTypes.MsSql:
                    provider = new MsSqlProvider(connectionString);
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