namespace Database.Initialization
{
    public static class ConnectionStringHelper
    {
        public static bool IsSQLite(string connectionString)
        {
            return connectionString.ToLower().Contains(".sqlite")
                || connectionString.ToLower().Contains(".db")
                || connectionString.ToLower().Contains(":memory:");
        }
    }
}
