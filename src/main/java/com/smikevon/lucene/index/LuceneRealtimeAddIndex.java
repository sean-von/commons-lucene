package com.smikevon.lucene.index;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
/**
 * 实现增加文档到索引的类
 * @author huangbin
 *
 */
public class LuceneRealtimeAddIndex extends AbstractLuceneIndex {
	private Document[] doc;

	public LuceneRealtimeAddIndex(String indexPath, Document... doc) {
		super(indexPath);
		this.doc = doc;
	}

    /**
     * 调用该构造函数必须自己控制writer的close方法
     * @param writer
     * @param doc
     */
	public LuceneRealtimeAddIndex(IndexWriter writer, Document... doc) {
		super(writer);
		this.doc = doc;
	}

	@Override
	protected final void addAllIndex(IndexWriter writer) throws CorruptIndexException, IOException {
		for(Document d:doc)
			writer.addDocument(d);
	}

	public final void makeIndex() throws LuceneIndexException {
		 makeIndex(0);
	}

}
