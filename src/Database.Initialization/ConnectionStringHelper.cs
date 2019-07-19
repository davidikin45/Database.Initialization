namespace Database.Initialization
{
    public static class ConnectionStringHelper
    {
        public static bool IsSQLite(string connectionString)
        {
            return connectionString.ToLower().Contains(".sqlite")
                || connectionString.ToLower().Contains(".db")
                || IsSQLiteInMemory(connectionString);
        }

        public static bool IsSQLiteInMemory(string connectionString)
        {
            return connectionString.ToLower().Contains(":memory:");
        }
    }
}
