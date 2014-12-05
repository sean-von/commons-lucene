package com.smikevon.lucene.index;
/**
 *
 * 生成索引时发生的异常
 * @author huangbin
 *
 */
public class LuceneIndexException extends Exception {
	private static final long serialVersionUID = 3029147915453709950L;

	public LuceneIndexException(Throwable cause){
		super(cause);
	}
	
	public LuceneIndexException(String info){
		super(info);
	}
}
