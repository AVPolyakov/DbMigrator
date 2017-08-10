using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using CsvHelper;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace DbMigrator
{
    class Program
    {
        static void Main()
        {
            var scriptDataList = GetScriptData().ToList();
            var dbVersions = GetDbVersions().ToDictionary(_ => _.ScriptName);
            if (!IsDevDatabase)
            {
                var nextAlreadyExecuted = false;
                foreach (var scriptData in scriptDataList.OrderByDescending(_ => _.Info.DbVersion))
                {
                    if (nextAlreadyExecuted && !dbVersions.ContainsKey(scriptData.Info.ScriptName))
                        throw new ApplicationException("База не является базой разработчика. Скрипты должны накатывать строго последовательно.");
                    if (dbVersions.ContainsKey(scriptData.Info.ScriptName))
                        nextAlreadyExecuted = true;
                }
            }
            foreach (var scriptData in scriptDataList.OrderBy(_ => _.Info.DbVersion))
            {
                DbVersion dbVersion;                
                if (!dbVersions.TryGetValue(scriptData.Info.ScriptName, out dbVersion))
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
                                Value = scriptData.Info.ScriptName
                            });
                            command.Parameters.AddWithValue("@DbVersion", scriptData.Info.DbVersion);
                            command.ExecuteNonQuery();
                        }
                        transaction.Commit();
                    }
                    Console.WriteLine($"{scriptData.Info.ScriptName} executed.");
                }
                else
                {
                    if (scriptData.Info.DbVersion != dbVersion.DBVersion)
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
                                    Value = scriptData.Info.ScriptName
                                });
                                command.Parameters.AddWithValue("@DbVersion", scriptData.Info.DbVersion);
                                command.ExecuteNonQuery();
                            }
                        }
                        Console.WriteLine($"{scriptData.Info.ScriptName} DB version updated.");
                    }
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

        private static IEnumerable<DbVersion> GetDbVersions()
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

        private static IEnumerable<ScriptData> GetScriptData()
        {
            foreach (var tuple in GetScriptInfos())
                using (var stream = ScriptsType.Assembly
                    .GetManifestResourceStream($"{ScriptsType.Namespace}.Scripts.{tuple.ScriptName}"))
                using (var reader = new StreamReader(stream))
                    yield return new ScriptData(tuple, reader.ReadToEnd());
        }

        private static IEnumerable<ScriptInfo> GetScriptInfos()
        {
            using (var stream = ScriptsType.Assembly.GetManifestResourceStream($"{ScriptsType.Namespace}.ScriptList.csv"))
            using (var reader = new StreamReader(stream))
            using (var csvReader = new CsvReader(reader))
                while (csvReader.Read())
                    yield return new ScriptInfo(
                        csvReader.GetField<int>("DbVersion"),
                        csvReader.GetField<string>("ScriptName"));
        }

        private static Type ScriptsType => typeof(Program);

        private static string ConnectionString => "Data Source=(local)\\SQL2014;Initial Catalog=Temp20170810_local;Integrated Security=True";
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
        public ScriptInfo Info { get; }
        public string Content { get; }

        public ScriptData(ScriptInfo info, string content)
        {
            Info = info;
            Content = content;
        }
    }

    public class ScriptInfo
    {
        public int DbVersion { get; }
        public string ScriptName { get; }

        public ScriptInfo(int dbVersion, string scriptName)
        {
            DbVersion = dbVersion;
            ScriptName = scriptName;
        }
    }
}