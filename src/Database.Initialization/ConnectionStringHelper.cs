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

        public static bool IsLiteDb(string connectionString)
        {
            return connectionString.ToLower().Contains(".litedb")
                || connectionString.ToLower().Contains(".db")
                || IsLiteDbInMemory(connectionString);
        }

        public static bool IsLiteDbInMemory(string connectionString)
        {
            return string.IsNullOrEmpty(connectionString) || connectionString.ToLower().Contains(":memory:");
        }
    }
}
