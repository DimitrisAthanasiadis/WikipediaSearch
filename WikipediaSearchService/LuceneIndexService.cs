using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using System;
using System.Collections.Generic;

namespace WikipediaSearchService
{
    public interface Indexer : IDisposable
    {
        void Index(IEnumerable<Field> fields);
    }

    public sealed class LuceneIndexService : IDisposable
    {
        const bool READONLY_MODE = true;
        const Lucene.Net.Util.Version Version = Lucene.Net.Util.Version.LUCENE_30;
        private Analyzer analyzer = new StandardAnalyzer(Version);
        private FSDirectory indexDirectory;
        private IndexWriter writer;
        private bool recreateIfExists;
        private string path;

        public LuceneIndexService(string path, bool recreateIfExists = false)
        {
            this.path = path;
            this.recreateIfExists = recreateIfExists;
        }

        private FSDirectory Directory 
        {
            get
            {
                if (indexDirectory == null) 
                {
                    if (recreateIfExists && System.IO.Directory.Exists(path))
                    {
                        System.IO.Directory.Delete(path, true);
                    }

                    indexDirectory = FSDirectory.Open(path);
                }

                return indexDirectory;
            }
        }

        public long Count()
        {
            using (var reader = IndexReader.Open(Directory, READONLY_MODE))
            {
                return reader.NumDocs();
            }
        }

        public Indexer GetIndexer()
        {
            return new IndexerImpl(Directory, analyzer, recreateIfExists);
        }
        
        public IEnumerable<Tuple<float, Document>> Search(string text, string defaultField = "title", int maxResultCount = 500)
        {
            var parser = new Lucene.Net.QueryParsers.QueryParser(Version, defaultField, analyzer);
            var qry = parser.Parse(text);

            using (var sercher = new IndexSearcher(IndexReader.Open(Directory, READONLY_MODE)))//true opens the index in read only mode
            {
                var hits = sercher.Search(qry, maxResultCount);

                foreach (var d in hits.ScoreDocs)
                {
                    var doc = sercher.Doc(d.Doc);
                    yield return new Tuple<float, Document>(d.Score, doc);
                }
            }
        }

        public void Close()
        {
            writer.Flush(false, false, false);
            writer.Dispose();
            indexDirectory.Dispose();
        }

        public void Dispose()
        {
            if (indexDirectory != null) indexDirectory.Dispose();
        }

        private class IndexerImpl : Indexer
        {
            private IndexWriter writer;

            public IndexerImpl(FSDirectory indexDirectory, Analyzer analyzer, bool recreateIfExists) 
            {
                writer = new IndexWriter(indexDirectory, analyzer, recreateIfExists, new IndexWriter.MaxFieldLength(IndexWriter.DEFAULT_MAX_FIELD_LENGTH));
            }

            public void Index(IEnumerable<Field> fields)
            {
                Document doc = new Document();
                foreach (var f in fields)
                    doc.Add(f);

                writer.AddDocument(doc);
            }
            
            public void Dispose()
            {
                writer.Dispose();
            }
        }
    }
}
