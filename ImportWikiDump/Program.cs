using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using WikipediaSearchService;

namespace ImportWikiDump
{
    public class Program
    {
        private static LuceneIndexService indexService = new LuceneIndexService(@"C:\temp");
        private static const string path = @"enwiki-20140811-pages-articles-multistream.xml\enwiki-20140811-pages-articles-multistream.xml";

        public static void Main() 
        {
            var sw = Stopwatch.StartNew();
            IndexWithLucene();
            sw.Stop();
            Console.WriteLine(sw.Elapsed.TotalMinutes);
            Console.Read();
        }

        private static void IndexWithLucene()
        {
            long counter = 0;
            using (var indexer = indexService.GetIndexer())
            {
                using (var reader = XmlReader.Create(path))
                {
                    while (reader.Read())
                    {
                        if (reader.IsStartElement("page"))
                        {
                            try
                            {
                                // PARSE WIKI PAGE
                                dynamic page = ReadElement(reader);

                                // INDEX
                                Field id = new Field("id", page.id, Field.Store.YES, Field.Index.NOT_ANALYZED);
                                Field title = new Field("title", page.title, Field.Store.YES, Field.Index.ANALYZED);
                                Field revision = new Field("revision", page.revision.text, Field.Store.YES, Field.Index.ANALYZED);
                         
                                indexer.Index(new[] { title, revision, id });

                                if (counter++ % 1000 == 0) Console.WriteLine(counter);
                            }
                            catch (Exception ex) 
                            {
                                Console.WriteLine("Exception " + ex.ToString());
                            }
                        }
                    }
                }
            }
        }
     
        private static Object ReadElement(XmlReader reader)
        {
            var name = reader.Name;
            IDictionary<string, object> result = new ExpandoObject();

            while (reader.Read()) 
            {
                if (reader.IsEmptyElement) continue;
                else if (reader.NodeType == XmlNodeType.EndElement && reader.Name == name)
                    return result;
                else if (reader.NodeType == XmlNodeType.EndElement)
                    continue;
                else if (reader.NodeType == XmlNodeType.Text)
                    return reader.Value;
                else if (reader.Name != "")
                    result[reader.Name] = ReadElement(reader);
            }

            throw new Exception();
        }
    }
}
