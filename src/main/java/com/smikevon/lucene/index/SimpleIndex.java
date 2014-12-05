package com.smikevon.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.smikevon.lucene.search.SimpleQuery;

/**
 *
 * 封装了对索引操作的添加文档、更新文档及删除文档方法
 *
 *	<p>添加文档<br>
 SimpleIndex si = new SimpleIndex(indexPath);<br>
 si.index("name", ...);<br>
 si.store("address", ...);<br>
 ...<br>
 si.addIndex()</p>

 <p>更新文档<br>
 SimpleIndex si = new SimpleIndex(indexPath);<br>
 si.index("name", ...);<br>
 si.store("address", ...);<br>
 ...<br>
 si. updateIndex("name", "张三");</p>

 <p>删除文档<br>
 SimpleIndex si = new SimpleIndex(indexPath);<br>
 si.deleteIndex("name", "张三");</p>

 <p>批量操作<br>
 SimpleIndex si = new SimpleIndex(indexPath);<br>
 for (i = 0; i < 5000; i++) {<br>
 &nbsp;&nbsp;si.index("name", ...);<br>
 &nbsp;&nbsp;si.store("adress", ...);<br>
 &nbsp;&nbsp;...<br>
 &nbsp;&nbsp;si.batchAdd();<br>
 }<br>
 si.closeBatch();</p>
 *
 * @author huangbin
 */
public class SimpleIndex extends DocParam {
	protected Logger logger = LoggerFactory.getLogger(this.getClass());

	private String indexPath;
	private AbstractLuceneIndex index;
	private IndexWriter _writer = null;
	private Long batchStart;

	public SimpleIndex(String indexPath) {
		this.indexPath = indexPath;
	}

	public final void addIndex() throws LuceneIndexException {
		index = new LuceneRealtimeAddIndex(indexPath, this.doc);
		index.makeIndex();
	}

	public final void updateIndex(String updateFieldName, String updateFieldValue) throws LuceneIndexException {
		index = new LuceneRealtimeUpdateIndex(indexPath, updateFieldName, updateFieldValue, this.doc);
		index.makeIndex();
	}

	public final void updateIndex(String updateFieldName) throws LuceneIndexException {
		updateIndex(updateFieldName, this.doc.get(updateFieldName));
	}

	public final void deleteIndex(String deleteFieldName, String deleteFieldValue) throws LuceneIndexException {
		index = new LuceneRealtimeDeleteIndex(indexPath, new SimpleQuery(indexPath).and(deleteFieldName, deleteFieldValue));
		index.makeIndex();
	}

	public void batchAdd() throws LuceneIndexException {
		check();

		index = new LuceneRealtimeAddIndex(_writer, this.doc);
		index.makeIndex();
		this.doc = new Document();
	}

	public void batchAdd(DocParam... docParam) throws LuceneIndexException {
		check();

		for (DocParam doc : docParam) {
			index = new LuceneRealtimeAddIndex(_writer, doc.doc);
			index.makeIndex();
		}
	}

	public void batchUpdate(String updateFieldName, String updateFieldValue) throws LuceneIndexException {
		check();

		index = new LuceneRealtimeUpdateIndex(_writer, updateFieldName, updateFieldValue, this.doc);
		index.makeIndex();
		this.doc = new Document();
	}

	public void batchUpdate(String updateFieldName) throws LuceneIndexException {
		batchUpdate(updateFieldName, this.doc.get(updateFieldName));
	}

	public void batchDelete(String deleteFieldName, String deleteFieldValue) throws LuceneIndexException {
		check();

		index = new LuceneRealtimeDeleteIndex(_writer,
				new SimpleQuery(indexPath).and(deleteFieldName, deleteFieldValue));
		index.makeIndex();
	}

	public void batchDelete(SimpleQuery query) throws LuceneIndexException {
		check();

		index = new LuceneRealtimeDeleteIndex(_writer, query);
		index.makeIndex();
	}

	public void batchDeleteAll() throws LuceneIndexException {
		check();

		index = new LuceneRealtimeDeleteIndex(_writer, true);
		index.makeIndex();
	}

	private void check() throws LuceneIndexException {
		if (_writer == null) {
			_writer = AbstractLuceneIndex.getIndexWriter(indexPath);
		}
		if (batchStart == null) {
			batchStart = System.currentTimeMillis();
		}
	}

	public void commitBatch() throws LuceneIndexException {
		if (_writer != null) {
			try {
				_writer.commit();
				if (batchStart != null) {
					logger.info("===> ������ʱ:{} ����.", System.currentTimeMillis() - batchStart);
					batchStart = null;
				}
			} catch (Exception e) {
				throw new LuceneIndexException(e);
			}
		} else
			throw new LuceneIndexException("IndexWriter is null!");
	}

	public void rollbackBatch() throws LuceneIndexException {
		if (_writer != null) {
			try {
				_writer.rollback();
				batchStart = null;
			} catch (Exception e) {
				throw new LuceneIndexException(e);
			}
		} else
			throw new LuceneIndexException("IndexWriter is null!");
	}

	public void closeBatch() throws LuceneIndexException {
		if (_writer != null) {
			AbstractLuceneIndex.releaseIndexWriter(_writer);

			if (batchStart != null) {
				logger.info("===> ������ʱ:{} ����.", System.currentTimeMillis() - batchStart);
				batchStart = null;
			}
			_writer = null;
		} else
			throw new LuceneIndexException("IndexWriter is null!");
	}

}
