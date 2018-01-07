using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DapperTest
{
    class ScopedTimer : IDisposable
    {
        public void Dispose()
        {
            Console.WriteLine(DateTime.Now - _begin);
        }

        private DateTime _begin = DateTime.Now;
    }

    class Program
    {
        static void Main(string[] args)
        {
            var builder = new SqlConnectionStringBuilder()
            {
                DataSource = "test.db",
            };

            TypeMapper.SetSnakeToCamelCase<Model>();
            TypeMapper.SetSnakeToCamelCase<Texture>();
            TypeMapper.SetSnakeToCamelCase<ModelTextureId>();

#if false
            using (var conn = new SQLiteConnection(builder.ToString()))
            {
                conn.Open();
                conn.Execute("PRAGMA foreign_keys = ON");

                QueryBuilder.CreateTable<Model>(conn);
                QueryBuilder.CreateTable<Texture>(conn);

                conn.Execute("DROP TABLE IF EXISTS `model_texture_ids`");
                conn.Execute(string.Format(@"
CREATE TABLE `model_texture_ids` (
    `model_id` INTEGER,
    `texture_id` INTEGER,
    FOREIGN KEY(`model_id`) REFERENCES `models`(`id`) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY(`texture_id`) REFERENCES `textures`(`id`) ON DELETE CASCADE ON UPDATE CASCADE,
    UNIQUE(`model_id`, `texture_id`)
)
"));
            }
#endif

            using (var conn = new SQLiteConnection(builder.ToString()))
            {
                conn.Open();
                conn.Execute("PRAGMA foreign_keys = ON");

                var iterations = 100000;
                var ids = Enumerable.Range(0, iterations).OrderBy(_ => Guid.NewGuid()).ToArray();
                var rand = new Random(0x12345678);

#if false
                var models = Enumerable.Range(0, iterations).Select(x => new Model() { Name = $"model_{x}" }).ToArray();
                var textures = Enumerable.Range(0, iterations).Select(x => new Texture() { Name = $"texture_{x}" }).ToArray();

                foreach (var model in models)
                {
                    var textureIds = Enumerable.Range(0, rand.Next(0, 5))
                        .Select(_ => rand.Next(0, iterations))
                        .Distinct();
                    model.Textures = textureIds.Select(x => textures[x]).ToArray();
                }

                using (new ScopedTimer())
                using (var trans = conn.BeginTransaction())
                {
                    foreach (var model in models)
                    {
                        conn.Execute("INSERT INTO `models` (`name`) values(@name)", new { name = model.Name });
                        model.Id = (int)conn.ExecuteScalar<long>("SELECT last_insert_rowid()");
                    }

                    foreach (var texture in textures)
                    {
                        conn.Execute("INSERT INTO `textures` (`name`) values(@name)", new { name = texture.Name });
                        texture.Id = (int)conn.ExecuteScalar<long>("SELECT last_insert_rowid()");
                    }

                    foreach (var model in models)
                    {
                        foreach (var texture in model.Textures)
                        {
                            conn.Execute(
                                "INSERT INTO `model_texture_ids` (`model_id`, `texture_id`) values (@model_id, @texture_id)",
                                new { model_id = model.Id, texture_id = texture.Id });
                        }
                    }

                    trans.Commit();
                }

                using (new ScopedTimer())
                using (var trans = conn.BeginTransaction())
                {
                    for (var i = 0; i < iterations; ++i)
                    {
                        var model = models[ids[i]];
                        conn.Execute("UPDATE `models` SET `name`=@new_name WHERE `id`=@id", new { id = model.Id, new_name = $"{model.Name}_1" });
                    }
                    trans.Commit();
                }
#endif

                GC.Collect();

                Model[] models;
                using (new ScopedTimer())
                {
                    var query = @"
SELECT * FROM `models` AS `m`
LEFT JOIN `model_texture_ids` AS `mt` ON `mt`.`model_id` = `m`.`id`
LEFT JOIN `textures` AS `t` ON `t`.`id` = `mt`.`texture_id`
";

                    var modelCache = new Dictionary<int, Model>();
                    var textureCache = new Dictionary<int, Texture>();

                    models = conn.Query<Model, ModelTextureId, Texture, Model>(
                        query,
                        (m, _, t) =>
                        {
                            var isNew = false;

                            Model model;
                            if (!modelCache.TryGetValue(m.Id, out model))
                            {
                                model = m; 
                                model.Textures = new List<Texture>();
                                modelCache[m.Id] = model;
                                isNew = true;
                            }

                            if (t != null)
                            {
                                Texture tex;
                                if (!textureCache.TryGetValue(t.Id, out tex))
                                {
                                    tex = t;
                                    textureCache[t.Id] = tex;
                                }

                                model.Textures.Add(tex);
                            }

                            return isNew ? model : null;
                        },
                        splitOn: "id,model_id,id")
                        .Where(x => x != null)
                        .ToArray();
                }
                Console.WriteLine(models.Length);
            }

#if false
            using (new ScopedTimer())
            {
                using (var conn = new ThreadLocal<SQLiteConnection>(() => { var item = new SQLiteConnection(builder.ToString()); item.Open(); return item; }, true))
                {
                    Parallel.For(0, 100, i =>
                    {
                        Thread.Sleep(100);
                        using (var trans = conn.Value.BeginTransaction())
                        {
                            conn.Value.Execute("INSERT INTO `test_table` (`name`, `test_name`) values(@name, @test_name)", new { name = Guid.NewGuid().ToString(), test_name = "test_" + Guid.NewGuid().ToString() });
                        }
                    });

                    foreach (var item in conn.Values)
                    {
                        item.Dispose();
                    }
                }
            }

            using (new ScopedTimer())
            {
                using (var conn = new SQLiteConnection(builder.ToString()))
                {
                    conn.Open();
                    using (var trans = conn.BeginTransaction())
                    {
                        for (var i = 0; i < 100; ++i)
                        {
                            Thread.Sleep(100);
                            conn.Execute("INSERT INTO `test_table` (`name`, `test_name`) values(@name, @test_name)", new { name = Guid.NewGuid().ToString(), test_name = "test_" + Guid.NewGuid().ToString() });
                        }
                    }
                }
            }

            using (new ScopedTimer())
            {
                Parallel.For(0, 100, i => Thread.Sleep(100));
                using (var conn = new SQLiteConnection(builder.ToString()))
                {
                    conn.Open();
                    using (var trans = conn.BeginTransaction())
                    {
                        for (var i = 0; i < 100; ++i)
                        {
                            conn.Execute("INSERT INTO `test_table` (`name`, `test_name`) values(@name, @test_name)", new { name = Guid.NewGuid().ToString(), test_name = "test_" + Guid.NewGuid().ToString() });
                        }
                        trans.Commit();
                    }
                }
            }

            using (var conn = new SQLiteConnection(builder.ToString()))
            {
                SqlMapper.SetTypeMap(typeof(TestTable), new CustomPropertyTypeMap(typeof(TestTable), (type, name) =>
                {
                    // snake_case -> CamelCase
                    var propName = string.Join("", name.Split('_').Select(x => (x.Length > 1) ? x[0].ToString().ToUpper() + x.Substring(1) : x.ToUpper()));
                    return type.GetProperty(propName);
                }));

                IEnumerable<TestTable> data;
                using (new ScopedTimer())
                {
                    data = conn.Query<TestTable>("SELECT * from `test_table`");
                }
                Console.WriteLine(data.Count());
            }
#endif

            Console.WriteLine("complete!!");
            Console.ReadKey();
        }
    }
}
