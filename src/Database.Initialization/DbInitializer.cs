using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Database.Initialization
{
    public static class DbInitializer
    {
        public static async Task<bool> ExistsAsync(string connectionString, CancellationToken cancellationToken = default)
        {
            try
            {
                await TestConnectionAsync(connectionString, cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static async Task<bool> ExistsAsync(DbConnection existingConnection, CancellationToken cancellationToken = default)
        {
            try
            {
                await TestConnectionAsync(existingConnection, cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static async Task TestConnectionAsync(string connectionString, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new Exception("Connection String empty");
            }
            else if (ConnectionStringHelper.IsSQLite(connectionString))
            {
                var builder = new SqliteConnectionStringBuilder(connectionString);
                builder.Mode = SqliteOpenMode.ReadOnly;

                using (var conn = new SqliteConnection(builder.ConnectionString))
                {
                    await TestConnectionAsync(conn).ConfigureAwait(false);
                }
            }
            else
            {
                using (var conn = new Microsoft.Data.SqlClient.SqlConnection(connectionString))
                {
                    await TestConnectionAsync(conn).ConfigureAwait(false);
                }
            }
        }

        public static Task TestConnectionAsync(DbConnection existingConnection, CancellationToken cancellationToken = default)
        {
            return existingConnection.OpenAsync(cancellationToken);
        }

        public static async Task<bool> HasTablesAsync(string connectionString)
        {
            var count = await TableCountAsync(connectionString);
            return count != 0;
        }

        public static async Task<long> TableCountAsync(string connectionString, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                return 0;
            }
            else if (ConnectionStringHelper.IsSQLite(connectionString))
            {
                var builder = new SqliteConnectionStringBuilder(connectionString);
                builder.Mode = SqliteOpenMode.ReadOnly;
                using (var conn = new SqliteConnection(builder.ConnectionString))
                {
                    return await TableCountAsync(conn, cancellationToken);
                }
            }
            else
            {
                using (var conn = new Microsoft.Data.SqlClient.SqlConnection(connectionString))
                {
                    return await TableCountAsync(conn, cancellationToken);
                }
            }
        }

        public static async Task<long> TableCountAsync(DbConnection existingConnection, CancellationToken cancellationToken = default)
        {
            if (existingConnection is SqliteConnection)
            {
                var count = await ExecuteScalarAsync<long>(existingConnection,
                    "SELECT COUNT(*) FROM \"sqlite_master\" WHERE \"type\" = 'table' AND \"rootpage\" IS NOT NULL AND \"name\" != 'sqlite_sequence';", cancellationToken).ConfigureAwait(false);

                return count;
            }
            else if (existingConnection is System.Data.SqlClient.SqlConnection || existingConnection is Microsoft.Data.SqlClient.SqlConnection)
            {
                var count = await ExecuteScalarAsync<int>(existingConnection,
                   "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", cancellationToken).ConfigureAwait(false);

                return Convert.ToInt64(count);
            }
            else
            {
                throw new Exception("Unsupported Connection");
            }
        }

        public static async Task<bool> TableExistsAsync(string connectionString, string tableName, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                return false;
            }
            else if (ConnectionStringHelper.IsSQLite(connectionString))
            {
                var builder = new SqliteConnectionStringBuilder(connectionString);
                builder.Mode = SqliteOpenMode.ReadOnly;
                using (var conn = new SqliteConnection(builder.ConnectionString))
                {
                    return await TableExistsAsync(conn, tableName, cancellationToken);
                }
            }
            else
            {
                using (var conn = new SqliteConnection(connectionString))
                {
                    return await TableExistsAsync(conn, tableName, cancellationToken);
                }
            }
        }

        public static async Task<bool> TableExistsAsync(DbConnection existingConnection, string tableName, CancellationToken cancellationToken = default)
        {

            if (existingConnection is SqliteConnection)
            {
                var exists = await ExecuteScalarAsync<long>(existingConnection,
                $@"SELECT CASE WHEN EXISTS (
                            SELECT * FROM ""sqlite_master"" WHERE ""type"" = 'table' AND ""rootpage"" IS NOT NULL AND ""tbl_name"" = '{tableName}'
                        )
                        THEN CAST(1 AS BIT)
                        ELSE CAST(0 AS BIT) END;", cancellationToken).ConfigureAwait(false);

                return exists == 1;
            }
            else if (existingConnection is System.Data.SqlClient.SqlConnection || existingConnection is Microsoft.Data.SqlClient.SqlConnection)
            {
                var exists = await ExecuteScalarAsync<bool>(existingConnection,
                    $@"SELECT CASE WHEN EXISTS (
                            SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = '{tableName}'
                        )
                        THEN CAST(1 AS BIT)
                        ELSE CAST(0 AS BIT) END;", cancellationToken).ConfigureAwait(false);

                return exists;
            }
            else
            {
                throw new Exception("Unsupported Connection");
            }
        }

        public static async Task<List<(string Schema, string TableName)>> TablesAsync(string connectionString, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                return new List<(string Schema, string TableName)>();
            }
            else if (ConnectionStringHelper.IsSQLite(connectionString))
            {
                var builder = new SqliteConnectionStringBuilder(connectionString);
                builder.Mode = SqliteOpenMode.ReadOnly;
                using (var conn = new SqliteConnection(builder.ConnectionString))
                {
                    return await TablesAsync(conn, cancellationToken);
                }
            }
            else
            {
                using (var conn = new Microsoft.Data.SqlClient.SqlConnection(connectionString))
                {
                    return await TablesAsync(conn, cancellationToken);
                }
            }
        }

        public static async Task<List<(string Schema, string TableName)>> TablesAsync(DbConnection existingConnection, CancellationToken cancellationToken = default)
        {
            if (existingConnection is SqliteConnection)
            {
                var tableNames = await ExecuteQueryAsync<(string Schema, string TableName)>(existingConnection,
                    "SELECT * FROM \"sqlite_master\" WHERE \"type\" = 'table' AND \"rootpage\" IS NOT NULL AND \"name\" != 'sqlite_sequence';",
                    row => ("", (string)row["tbl_name"]), cancellationToken).ConfigureAwait(false);

                return tableNames;
            }
            else if (existingConnection is System.Data.SqlClient.SqlConnection || existingConnection is Microsoft.Data.SqlClient.SqlConnection)
            {
                var tableNames = await ExecuteQueryAsync<(string Schema, string TableName)>(existingConnection,
                       "SELECT TABLE_SCHEMA + '.' + TABLE_NAME as tbl_name, TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME",
                       row => ((string)row["TABLE_SCHEMA"], (string)row["TABLE_NAME"]), cancellationToken).ConfigureAwait(false);

                return tableNames;
            }
            else
            {
                throw new Exception("Unsupported Connection");
            }
        }

        public static async Task<bool> EnsureCreatedAsync(string connectionString, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                return false;
            }
            else if (ConnectionStringHelper.IsSQLite(connectionString))
            {
                using (var conn = new SqliteConnection(connectionString))
                {
                    return await EnsureCreatedAsync(conn, cancellationToken);
                }
            }
            else
            {
                using (var conn = new Microsoft.Data.SqlClient.SqlConnection(connectionString))
                {
                    return await EnsureCreatedAsync(conn, cancellationToken);
                }
            }
        }

        public static async Task<bool> EnsureCreatedAsync(DbConnection existingConnection, CancellationToken cancellationToken = default)
        {
            if (existingConnection is SqliteConnection)
            {
                bool exists = false;

                if (existingConnection.ConnectionString.Contains(":memory"))
                {
                    exists = await DbInitializer.ExistsAsync(existingConnection, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    exists = await DbInitializer.ExistsAsync(existingConnection.ConnectionString, cancellationToken).ConfigureAwait(false);
                }

                if (!exists)
                {
                    using (var conn = new SqliteConnection(existingConnection.ConnectionString))
                    {
                        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                        return true;
                    }
                }
                else
                {
                    return false;
                }
            }
            else if (existingConnection is System.Data.SqlClient.SqlConnection || existingConnection is Microsoft.Data.SqlClient.SqlConnection)
            {
                bool exists = await DbInitializer.ExistsAsync(existingConnection.ConnectionString, cancellationToken);

                if (!exists)
                {
                    var masterConnectiongStringBuilder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(existingConnection.ConnectionString);
                    var dbName = masterConnectiongStringBuilder.InitialCatalog;

                    masterConnectiongStringBuilder.InitialCatalog = "master";
                    masterConnectiongStringBuilder.AttachDBFilename = "";

                    using (var masterConnection = new Microsoft.Data.SqlClient.SqlConnection(masterConnectiongStringBuilder.ConnectionString))
                    {
                        await ExecuteCommandAsync(masterConnection, $@"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'{dbName}') 
                            BEGIN
                                    CREATE DATABASE [{dbName}];

                                    IF SERVERPROPERTY('EngineEdition') <> 5
                                    BEGIN
                                        ALTER DATABASE [{dbName}] SET READ_COMMITTED_SNAPSHOT ON;
                                    END;
                            END", cancellationToken).ConfigureAwait(false);

                        ClearPool(existingConnection);
                    }

                    //https://github.com/dotnet/efcore/blob/bde2b140d6f4cf94d6d1285d402941e20193ec60/src/EFCore.SqlServer/Storage/Internal/SqlServerDatabaseCreator.cs
                    var retryOnNotExists = true;
                    var giveUp = DateTime.UtcNow + TimeSpan.FromMinutes(1);
                    var retryDelay = TimeSpan.FromMilliseconds(500);

                    while(true)
                    {
                        exists = await ExistsAsync(existingConnection, cancellationToken);
                        if (exists)
                            return true;
                        if (!retryOnNotExists)
                            return false;
                        else if (DateTime.UtcNow > giveUp)
                            throw new Exception($"Failed to create database: {dbName}");

                        await Task.Delay(retryDelay, cancellationToken);
                    }
                }
                else
                {
                    return false;
                }
            }
            else
            {
                throw new Exception("Unsupported Connection");
            }
        }

        // Clear connection pool for the database connection since after the 'create database' call, a previously
        // invalid connection may now be valid.
        private static void ClearPool(DbConnection existingConnection) { 
            if(existingConnection is Microsoft.Data.SqlClient.SqlConnection sqlConnection)
            {
                Microsoft.Data.SqlClient.SqlConnection.ClearPool(sqlConnection);
            }
            else if (existingConnection is System.Data.SqlClient.SqlConnection systemSqlConnection)
            {
                System.Data.SqlClient.SqlConnection.ClearPool(systemSqlConnection);
            }
        }

        public static async Task<bool> EnsureDestroyedAsync(string connectionString, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                return false;
            }
            else if (ConnectionStringHelper.IsSQLite(connectionString))
            {
                using (var conn = new SqliteConnection(connectionString))
                {
                    return await EnsureDestroyedAsync(conn, cancellationToken);
                }
            }
            else
            {
                using (var conn = new Microsoft.Data.SqlClient.SqlConnection(connectionString))
                {
                    return await EnsureDestroyedAsync(conn, cancellationToken);
                }
            }
        }

        public static async Task<bool> EnsureDestroyedAsync(DbConnection existingConnection, CancellationToken cancellationToken = default)
        {
            if (existingConnection is SqliteConnection)
            {
                var builder = new SqliteConnectionStringBuilder(existingConnection.ConnectionString);

                if (!string.IsNullOrEmpty(builder.DataSource))
                {
                    if (File.Exists(builder.DataSource))
                    {
                        File.Delete(builder.DataSource);
                        return true;
                    }
                }
            }
            else if (existingConnection is System.Data.SqlClient.SqlConnection || existingConnection is Microsoft.Data.SqlClient.SqlConnection)
            {
                var masterConnectiongStringBuilder = new SqlConnectionStringBuilder(existingConnection.ConnectionString);
                var dbName = masterConnectiongStringBuilder.InitialCatalog;
                masterConnectiongStringBuilder.InitialCatalog = "master";

                string mdfFileName = string.Empty;
                string logFileName = string.Empty;
                if (!string.IsNullOrEmpty(masterConnectiongStringBuilder.AttachDBFilename))
                {
                    mdfFileName = Path.GetFullPath(masterConnectiongStringBuilder.AttachDBFilename);
                    var name = Path.GetFileNameWithoutExtension(mdfFileName);
                    logFileName = Path.ChangeExtension(mdfFileName, ".ldf");
                    var logName = name + "_log";
                    // Match default naming behavior of SQL Server
                    logFileName = logFileName.Insert(logFileName.Length - ".ldf".Length, "_log");
                }

                masterConnectiongStringBuilder.AttachDBFilename = "";

                var masterConnectionString = masterConnectiongStringBuilder.ConnectionString;

                using (var masterConnection = new Microsoft.Data.SqlClient.SqlConnection(masterConnectionString))
                {

                    var fileNames = await ExecuteQueryAsync(masterConnection, @"
                SELECT [physical_name] FROM [sys].[master_files]
                WHERE [database_id] = DB_ID('" + dbName + "')",
                  row => (string)row["physical_name"], cancellationToken).ConfigureAwait(false);

                    if (fileNames.Any())
                    {
                        await ExecuteCommandAsync(masterConnection, $@"
                        ALTER DATABASE [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                        DROP DATABASE [{dbName}];", cancellationToken).ConfigureAwait(false);

                        //ExecuteSqlCommand(masterConnectiongString, $@"
                        //    ALTER DATABASE [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                        //    EXEC sp_detach_db '{dbName}'");
                        ////To remove a database from the current server without deleting the files from the file system, use sp_detach_db.

                        foreach (var file in fileNames)
                        {
                            if (File.Exists(file))
                            {
                                File.Delete(file);
                            }
                        }
                        return true;
                    }

                    if (!string.IsNullOrEmpty(mdfFileName) && File.Exists(mdfFileName))
                    {
                        File.Delete(mdfFileName);
                        return true;
                    }

                    if (!string.IsNullOrEmpty(logFileName) && File.Exists(logFileName))
                    {
                        File.Delete(logFileName);
                        return true;
                    }
                }
            }
            else
            {
                throw new Exception("Unsupported Connection");
            }
            return false;
        }

        #region Existing Connection Helper Methods
        private static async Task ExecuteCommandAsync(
        DbConnection connection,
        string commandText,
        CancellationToken cancellationToken = default)
        {
            var opened = false;
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                opened = true;
            }

            try
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = commandText;
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                if (opened && connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
            }
        }

        private static async Task<TType> ExecuteScalarAsync<TType>(
       DbConnection connection,
       string queryText,
       CancellationToken cancellationToken = default)
        {
            TType result = default(TType);

            var opened = false;
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                opened = true;
            }

            try
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = queryText;
                    result = (TType)(await command.ExecuteScalarAsync(cancellationToken));
                }
            }
            finally
            {
                if (opened && connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
            }

            return result;
        }

        private static async Task<List<TType>> ExecuteQueryAsync<TType>(
        DbConnection connection,
        string queryText,
        Func<DbDataReader, TType> read,
       CancellationToken cancellationToken = default)
        {
            var result = new List<TType>();

            var opened = false;
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                opened = true;
            }

            try
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = queryText;
                    using (var reader = await command.ExecuteReaderAsync(cancellationToken))
                    {
                        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            result.Add(read(reader));
                        }
                    }
                }
            }
            finally
            {
                if (opened && connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
            }

            return result;
        }
        #endregion
    }
}
