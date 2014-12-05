package com.smikevon.lucene.index;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
/**
 *
 * 实现更新文档到索引的类
 * @author huangbin
 *
 */
public class LuceneRealtimeUpdateIndex extends AbstractLuceneIndex {
	private Document doc;
	private String updateFieldName;
	private String updateFieldValue;

	public LuceneRealtimeUpdateIndex(String indexPath, String updateFieldName, String updateFieldValue, Document doc) {
		super(indexPath);
		this.updateFieldName = updateFieldName;
		this.updateFieldValue = updateFieldValue;
		this.doc = doc;
	}

    /**
     * 调用该构造函数必须自己控制writer的close方法
     * @param writer
     * @param updateFieldName
     * @param updateFieldValue
     * @param doc
     */
	public LuceneRealtimeUpdateIndex(IndexWriter writer, String updateFieldName, String updateFieldValue, Document doc) {
		super(writer);
		this.updateFieldName = updateFieldName;
		this.updateFieldValue = updateFieldValue;
		this.doc = doc;
	}

	@Override
	protected final void addAllIndex(IndexWriter writer) throws CorruptIndexException, IOException {
		Term term = new Term(updateFieldName, updateFieldValue);
//		writer.deleteDocuments(new WildcardQuery(term));
//		writer.addDocument(doc);
		writer.updateDocument(term, doc);
	}

	public final void makeIndex() throws LuceneIndexException {
		 makeIndex(0);
	}

}
