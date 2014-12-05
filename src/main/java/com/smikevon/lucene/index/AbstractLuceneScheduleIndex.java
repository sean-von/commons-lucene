package com.smikevon.lucene.index;

import java.util.List;

import org.apache.lucene.index.IndexWriter;

import com.smikevon.lucene.LuceneConfig;
/**
 *
 * 为后台定时更新任务作准备的类,生成索引时会调用forceMerge方法
 * @author huangbin
 *
 * @param <T> 泛型对象类型
 */
public abstract class AbstractLuceneScheduleIndex<T> extends AbstractLuceneIndex {

    protected AbstractLuceneScheduleIndex(String indexPath) {
        super(indexPath);
    }

    private int limitForDev=0;

    public final void setLimitForDev(int limit){
        this.limitForDev=limit;
    }

    private Filter<T> filter;

    public final void setFilterForDev(Filter<T> filter) {
        this.filter = filter;
    }

    public static interface Filter<E> {
        boolean accept(E o);
    }

    @Override
    protected final void addAllIndex(IndexWriter writer) throws Exception {
        writer.deleteAll();

        List<T> productlist = null;
        int start = 0;
        int size = LuceneConfig.getSchedulePagenum();
        int count = 0;
        breakFor: while (true) {
            productlist = getList(start, size);
            if (productlist.size() == 0)
                break;
            if (writer != null) {
                if (productlist != null) {
                    for (int i = 0, n = productlist.size(); i < n; i++) {
                        if (limitForDev > 0 && count >= limitForDev) {
                            break breakFor;
                        }
                        if (filter != null) {
                            if (!filter.accept(productlist.get(i))) {
                                continue;
                            }
                        }
                        DocParam[] doc = getDocParam(productlist.get(i));
                        if (doc != null) {
                            for (DocParam d : doc)
                                writer.addDocument(d.getDocument());
                        }
                        count++;
                    }
                }
            }
            start += size;
        }
    }

    public final void makeIndex() throws LuceneIndexException {
        makeIndex(1);
    }

    /**
     * 由扩展类实现,该方法第一次调用会传参数start=0，size=1000，第二次start=1000,size=1000，第三次start=
     * 2000,size=1000... 一直调用到返回的集合为空
     * @param start 查询起始
     * @param size 查询个数
     * @return 泛型对象列表
     */
    protected abstract List<T> getList(int start, int size) throws Exception;

    /**
     * 由扩展类实现
     * @param obj
     * @return 将泛型对象转化成的DocParam数组
     * @throws Exception
     */
    public abstract DocParam[] getDocParam(T obj) throws Exception;

}
