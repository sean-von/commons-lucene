package com.smikevon.lucene.index;

/**
 * 和AbstractLuceneScheduleIndex类配合用来在索引上增加文档
 * @author huangbin
 *
 * @param <T>
 */
public class LuceneAppendIndex<T> {
	private AbstractLuceneScheduleIndex<T> schedule;

	public LuceneAppendIndex(AbstractLuceneScheduleIndex<T> index) {
		schedule = index;
	}

	public void append(T... obj) throws Exception{
		SimpleIndex si=new SimpleIndex(schedule.indexPath);
		for(T o:obj){
			si.batchAdd(schedule.getDocParam(o));
		}
		si.closeBatch();
	}
}