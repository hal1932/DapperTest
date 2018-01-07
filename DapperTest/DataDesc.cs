using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DapperTest
{
    public static class NamingConverter
    {
        public static string SnakeToCamel(string value)
        {
            return string.Join(string.Empty, value.Split('_').Select(x =>
                x.Length > 1 ? x.Substring(0, 1).ToUpper() + x.Substring(1) : x.ToUpper()));
        }

        public static string CamelToSnake(string value)
        {
            return new Regex("[A-Z]").Replace(value, m => "_" + m.Value.ToLower()).TrimStart('_');
        }
    }

    public static class TypeMapper
    {
        public static void SetSnakeToCamelCase<T>()
        {
            SqlMapper.SetTypeMap(typeof(T), new CustomPropertyTypeMap(typeof(T), (type, name) =>
            {
                var propName = NamingConverter.SnakeToCamel(name);
                return type.GetProperty(propName);
            }));
        }
    }

    public static class QueryBuilder
    {
        public static void CreateTable<T>(SQLiteConnection conn, bool clean = true)
        {
            var tableName = NamingConverter.CamelToSnake(typeof(T).Name) + "s";
            if (clean)
            {
                conn.Execute($"DROP TABLE IF EXISTS `{tableName}`");
            }

            var columnDefs = new List<string>();
            foreach (var prop in typeof(T).GetProperties())
            {
                var name = NamingConverter.CamelToSnake(prop.Name);

                var typeStr = default(string);
                if (prop.PropertyType == typeof(int))
                {
                    typeStr = "INTEGER";
                }
                else if (prop.PropertyType == typeof(string))
                {
                    typeStr = "TEXT";
                }
                if (typeStr == default(string))
                {
                    continue;
                }

                var columnDef = $"`{name}` {typeStr}";
                if (name == "id")
                {
                    columnDef += " PRIMARY KEY AUTOINCREMENT";
                }

                columnDefs.Add(columnDef);
            }

            conn.Execute($"CREATE TABLE `{tableName}` ({string.Join(",", columnDefs)})");
        }
    }

    [DebuggerDisplay("Id={Id}, Name={Name}")]
    public class Model
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public List<Texture> Textures { get; set; }
    }

    [DebuggerDisplay("Id={Id}, Name={Name}")]
    public class Texture
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    [DebuggerDisplay("ModelId={ModelId}, TextureId={TextureId}")]
    public class ModelTextureId
    {
        public int ModelId { get; set; }
        public int TextureId { get; set; }
    }
}
