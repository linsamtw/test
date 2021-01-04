using System;
using System.IO;
using System.Xml.Serialization;

namespace Sinopac.Shioaji
{
    public class Config
    {
        public string MY_ENV;


        [XmlIgnore] public const string FileName = "config.xml";

        [XmlIgnore] public static Config Instance { get; private set; }

        public Config()
        {
        }

        public static void Default()
        {
            Config.Instance = new Config();
        }

        public static void Load()
        {
            var serializer = new XmlSerializer(typeof(Config));
            using (var fStream = new FileStream(Config.FileName, FileMode.Open))
                Config.Instance = (Config) serializer.Deserialize(fStream);
        }

        // Saves the configuration to file.
        public void Save()
        {
            var serializer = new XmlSerializer(typeof(Config));

            using (var fStream = new FileStream(Config.FileName, FileMode.Create))
                serializer.Serialize(fStream, this);
        }
    }

    class MainClass
    {
        public static void Main(string[] args)
        {
            Config.Load();
            Config.Instance.MY_ENV = Environment.GetEnvironmentVariable("MY_ENV");

            Console.WriteLine("Saving config...");
            Config.Instance.Save();
        }
    }
}