using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using static DbMigrator.Program;

namespace DbMigrator
{
    static class Program
    {
        static void Main()
        {
            var scriptDataList = GetScriptData().ToList();
            var grouping = scriptDataList.GroupBy(_ => _.DbVersion).FirstOrDefault(g => g.Count() > 1);
            if (grouping != null)
            {
                var files = string.Join(Environment.NewLine, grouping.Select(_ => _.ScriptName));
                throw new ApplicationException($@"Дублирование версии {grouping.Key} в файлах:
{files}");
            }
            var dbVersions = GetDbVersions().ToDictionary(_ => _.ScriptName);
            if (!IsDevDatabase)
            {
                var nextAlreadyExecuted = false;
                foreach (var scriptData in scriptDataList.OrderByDescending(_ => _.DbVersion))
                {
                    if (nextAlreadyExecuted && !dbVersions.ContainsKey(scriptData.ScriptName))
                        throw new ApplicationException("База не является базой разработчика. Скрипты должны накатывать строго последовательно.");
                    if (dbVersions.ContainsKey(scriptData.ScriptName))
                        nextAlreadyExecuted = true;
                }
            }
            var orderBy = scriptDataList.OrderBy(_ => _.DbVersion).ToList();
            foreach (var scriptData in orderBy)
            {
                DbVersion dbVersion;
                if (dbVersions.TryGetValue(scriptData.ScriptName, out dbVersion))
                {
                    if (scriptData.DbVersion != dbVersion.DBVersion)
                    {
                        using (var connection = new SqlConnection(ConnectionString))
                        {
                            connection.Open();
                            using (var command = new SqlCommand())
                            {
                                command.Connection = connection;
                                command.CommandText = "UPDATE DbVersions SET DbVersion = @DbVersion WHERE ScriptName = @ScriptName";
                                command.Parameters.Add(new SqlParameter {
                                    ParameterName = "@ScriptName",
                                    Size = -1,
                                    Value = scriptData.ScriptName
                                });
                                command.Parameters.AddWithValue("@DbVersion", scriptData.DbVersion);
                                command.ExecuteNonQuery();
                            }
                        }
                        Console.WriteLine($"{scriptData.ScriptName} DB version updated.");
                    }
                }
            }
            foreach (var scriptData in orderBy)
            {
                if (!dbVersions.ContainsKey(scriptData.ScriptName))
                {
                    using (var connection = new SqlConnection(ConnectionString))
                    {
                        connection.Open();
                        var transaction = connection.BeginTransaction();
                        foreach (var batch in GetBatches(scriptData.Content).ToList())
                            using (var command = new SqlCommand())
                            {
                                command.Connection = transaction.Connection;
                                command.Transaction = transaction;
                                command.CommandText = batch;
                                command.ExecuteNonQuery();
                            }
                        using (var command = new SqlCommand())
                        {
                            command.Connection = transaction.Connection;
                            command.Transaction = transaction;
                            command.CommandText = "INSERT INTO DbVersions (ScriptName, DbVersion) VALUES (@ScriptName, @DbVersion)";
                            command.Parameters.Add(new SqlParameter {
                                ParameterName = "@ScriptName",
                                Size = -1,
                                Value = scriptData.ScriptName
                            });
                            command.Parameters.AddWithValue("@DbVersion", scriptData.DbVersion);
                            command.ExecuteNonQuery();
                        }
                        transaction.Commit();
                    }
                    Console.WriteLine($"{scriptData.ScriptName} executed.");
                }
            }
        }

        private static IEnumerable<string> GetBatches(string text)
        {
            var parser = new TSql120Parser(false);
            IList<ParseError> parseErrors;
            var scriptFragment = parser.Parse(new StringReader(text), out parseErrors);
            if (parseErrors.Count > 0)
            {
                var error = parseErrors[0];
                throw new ApplicationException($@"{error.Message}
Line={error.Line},
Column={error.Column}");
            }
            if (!(scriptFragment is TSqlScript))
                throw new ApplicationException();
            var sqlScript = (TSqlScript) scriptFragment;
            foreach (var sqlBatch in sqlScript.Batches)
                yield return text.Substring(sqlBatch.StartOffset, sqlBatch.FragmentLength);
        }

        private static bool IsDevDatabase => new SqlConnectionStringBuilder(ConnectionString).InitialCatalog.Contains("local");

        public static IEnumerable<DbVersion> GetDbVersions()
        {
            using (var connection = new SqlConnection(ConnectionString))
            using (var command = new SqlCommand())
            {
                command.Connection = connection;
                command.CommandText = "SELECT DbVersion, ScriptName FROM DbVersions";
                connection.Open();
                using (var reader = command.ExecuteReader())
                    while (reader.Read())
                        yield return new DbVersion(
                            reader.GetInt32(reader.GetOrdinal("DbVersion")),
                            reader.GetString(reader.GetOrdinal("ScriptName")));
            }
        }

        public static IEnumerable<ScriptData> GetScriptData()
        {
            var resourceNames = ScriptsType.Assembly.GetManifestResourceNames()
                .Where(_ => _.EndsWith(".sql", StringComparison.InvariantCultureIgnoreCase))
                .ToList();
            foreach (var resourceName in resourceNames)
                using (var stream = ScriptsType.Assembly.GetManifestResourceStream(resourceName))
                using (var reader = new StreamReader(stream))
                    yield return new ScriptData(resourceName, reader.ReadToEnd());
        }

        public static Type ScriptsType => typeof(Program);

        private static string ConnectionString => "Data Source=(local)\\SQL2014;Initial Catalog=Temp20170810_local;Integrated Security=True";

        public static IEnumerable<string> SplitToLines(this string text)
        {
            using (var reader = new StringReader(text))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                    yield return line;
            }
        }

        public const string Scripts = "Scripts";
    }

    public class DbVersion
    {
        public int DBVersion { get; }
        public string ScriptName { get; }

        public DbVersion(int dbVersion, string scriptName)
        {
            DBVersion = dbVersion;
            ScriptName = scriptName;
        }
    }

    public class ScriptData
    {
        public string ScriptName => ResourceName.Substring($"{ScriptsType.Namespace}.{Scripts}.".Length);

        public int DbVersion
        {
            get
            {
                if (!DbVersionOrNull.HasValue)
                {
                    var dbVersions = GetScriptData().ToList();
                    var nextVersion = dbVersions.Count == 0 
                        ? 1 
                        : dbVersions.Where(_ => _.DbVersionOrNull.HasValue).Max(_ => _.DbVersionOrNull.Value) + 1;
                    throw new ApplicationException($"В файле {ScriptName} не указана версия. Следующая версия {nextVersion}.");
                }
                return DbVersionOrNull.Value;
            }
        }

        private int? DbVersionOrNull
        {
            get
            {
                var firstOrDefault = Content.SplitToLines().FirstOrDefault();
                if (firstOrDefault == null) return null;
                const string value = "--";
                if (!firstOrDefault.StartsWith(value)) return null;
                int result;
                if (!int.TryParse(firstOrDefault.Substring(value.Length), out result)) return null;
                return result;
            }
        }

        public string ResourceName { get; }
        public string Content { get; }

        public ScriptData(string resourceName, string content)
        {
            ResourceName = resourceName;
            Content = content;
        }
    }
}