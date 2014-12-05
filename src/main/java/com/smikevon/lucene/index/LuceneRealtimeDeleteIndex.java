package com.smikevon.lucene.index;

import java.io.IOException;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.ParseException;

import com.smikevon.lucene.search.SimpleQuery;
/**
 *
 * 实现在索引中删除文档的类
 * @author huangbin
 *
 */
public class LuceneRealtimeDeleteIndex extends AbstractLuceneIndex {
	private SimpleQuery query;
	boolean delAll;

	public LuceneRealtimeDeleteIndex(String indexPath, SimpleQuery query) {
		super(indexPath);
		this.query = query;
	}

    /**
     * 调用该构造函数必须自己控制writer的close方法
     * @param writer
     * @param query
     */
	public LuceneRealtimeDeleteIndex(IndexWriter writer, SimpleQuery query) {
		super(writer);
		this.query = query;
	}
	
	public LuceneRealtimeDeleteIndex(IndexWriter writer, boolean delAll) {
		super(writer);
		this.delAll = delAll;
	}

	@Override
	protected final void addAllIndex(IndexWriter writer) throws CorruptIndexException, IOException, ParseException {
		if(delAll){
			writer.deleteAll();
		}else {
			writer.deleteDocuments(query.getQuery());
		}
	}

	public final void makeIndex() throws LuceneIndexException {
		 makeIndex(0);
	}

}
