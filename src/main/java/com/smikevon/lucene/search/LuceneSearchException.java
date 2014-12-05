package com.smikevon.lucene.search;
/**
 *
 * 查询索引时发生的异常
 * @author huangbin
 */
public class LuceneSearchException extends RuntimeException {
	private static final long serialVersionUID = 6344802572994207984L;

	public LuceneSearchException(Throwable cause){
		super(cause);
	}
	
	public LuceneSearchException(String info){
		super(info);
	}
}
