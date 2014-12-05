package com.smikevon.lucene.search;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.SortField;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 *
 * 表示查询条件的类
 * @author huangbin
 *
 */
public class SearchParam {
    private Multimap<String, Object> mustParam = HashMultimap.create();
    private Multimap<String, Object> includeParam = HashMultimap.create();
    private Multimap<String, Object> excludeParam = HashMultimap.create();

    private List<SearchParam> mustList = new ArrayList<SearchParam>();
    private List<SearchParam> includeList = new ArrayList<SearchParam>();
    private List<SearchParam> excludeList = new ArrayList<SearchParam>();
    private Map<String, QueryType> queryMethod = new HashMap<String, QueryType>();
    private Map<String, boolean[]> queryRange = new HashMap<String, boolean[]>();
    private int start;
    private int pageSize;
    private List<SortField> sortFields = new ArrayList<SortField>();

    public static enum QueryType {
        ANALYZED, WILDCARD, FUZZY, PREFIX, REGEXP, TEXT_RANGE, LONG_RANGE, DOUBLE_RANGE
    }

    public static enum LogicType {
        AND, OR, NOT
    }

    /**
     * 构造方法,若使用该构造方法，又没有调用setResultRange方法，则查询的结果中将只有总数而没有具体的数据
     */
    public SearchParam() {
    }

    /**
     * 构造方法
     *
     * @param start
     *            查询结果的开始位置
     * @param pageSize
     *            查询结果的最多数量
     */
    public SearchParam(int start, int pageSize) {
        this.start = start;
        this.pageSize = pageSize;
    }

    /**
     * 设置查询结果的范围
     *
     * @param start
     *             查询结果的开始位置
     * @param pageSize 查询结果的最多数量
     * @return 链式返回SearchParam对象
     */
    public SearchParam setSearchRange(int start, int pageSize) {
        this.start = start;
        this.pageSize = pageSize;
        return this;
    }

    private Multimap<String, Object> getParamMap(LogicType type) {
        if (type == LogicType.AND)
            return mustParam;
        else if (type == LogicType.OR)
            return includeParam;
        else if (type == LogicType.NOT)
            return excludeParam;
        else
            return null;
    }

    private List<SearchParam> getParamList(LogicType type) {
        if (type == LogicType.AND)
            return mustList;
        else if (type == LogicType.OR)
            return includeList;
        else if (type == LogicType.NOT)
            return excludeList;
        else
            return null;
    }

    private void setRange(String propName, QueryType queryType, boolean includeMin, boolean includeMax) {
        queryMethod.put(propName, queryType);
        queryRange.put(propName, new boolean[] { includeMin, includeMax });
    }

    /**
     * 增加查询条件 (没有设置QueryType,效果等同于QueryType.WILDCARD)
     *
     * @param propName 查询属性名
     * @param value  查询属性值
     * @param logicType 查询逻辑
     * @return 链式返回SearchParam对象
     */
    public SearchParam addParam(String propName, String value, LogicType logicType) {
        Multimap<String, Object> param = getParamMap(logicType);
        param.put(propName, value);
        return this;
    }

    /**
     * 增加查询条件
     *
     * @param propName 查询属性名
     * @param start
     *            查询属性起始值
     * @param end
     *            查询属性结束值
     * @param logicType 查询逻辑
     * @param includeMin 是否包含起始值
     * @param includeMax 是否包含结束值
     * @return 链式返回SearchParam对象
     */
    public SearchParam addParam(String propName, String start, String end, LogicType logicType, boolean includeMin,
                                boolean includeMax) {
        Multimap<String, Object> param = getParamMap(logicType);
        param.put(propName, new String[] { start, end });
        setRange(propName, QueryType.TEXT_RANGE, includeMin, includeMax);
        return this;
    }

    /**
     * 增加查询条件
     *
     * @param propName 查询属性名
     * @param start
     *             查询属性起始值
     * @param end
     *             查询属性结束值
     * @param logicType 查询逻辑
     * @param includeMin 是否包含起始值
     * @param includeMax 是否包含结束值
     * @return 链式返回SearchParam对象
     */
    public SearchParam addParam(String propName, Long start, Long end, LogicType logicType, boolean includeMin,
                                boolean includeMax) {
        Multimap<String, Object> param = getParamMap(logicType);
        param.put(propName, new Long[] { start, end });
        setRange(propName, QueryType.LONG_RANGE, includeMin, includeMax);
        return this;
    }

    /**
     * 增加查询条件
     *
     * @param propName 查询属性名
     * @param start
     *             查询属性起始值
     * @param end
     *             查询属性结束值
     * @param logicType 查询逻辑
     * @param includeMin 是否包含起始值
     * @param includeMax 是否包含结束值
     * @return 链式返回SearchParam对象
     */
    public SearchParam addParam(String propName, Double start, Double end, LogicType logicType, boolean includeMin,
                                boolean includeMax) {
        Multimap<String, Object> param = getParamMap(logicType);
        param.put(propName, new Double[] { start, end });
        setRange(propName, QueryType.DOUBLE_RANGE, includeMin, includeMax);
        return this;
    }

    /**
     * 增加查询条件：使用指定的查询方法(QueryType)，当查询方法为WILDCARD时可省略，即调用不用参数QueryType的addIncludeParam方法
     *
     * @param propName 查询属性名
     * @param type
     *            查询方法
     * @param value 查询属性值
     * @param logicType 查询逻辑
     * @return 链式返回SearchParam对象
     */
    public SearchParam addParam(String propName, String value, QueryType type, LogicType logicType) {
        Multimap<String, Object> param = getParamMap(logicType);
        param.put(propName, value);
        queryMethod.put(propName, type);
        return this;
    }

    /**
     * 增加查询条件：将传入参数对象的查询条件用logicType的方式合并到返回的SearchParam对象
     * @param param SearchParam对象
     * @param logicType 查询逻辑
     * @return 链式返回SearchParam对象
     */
    public SearchParam addParam(SearchParam param, LogicType logicType) {
        List<SearchParam> list = getParamList(logicType);
        list.add(param);
        return this;
    }

    /**
     * 添加排序字段
     *
     * @param sortField 排序字段
     * @return 链式返回SearchParam对象
     */
    public SearchParam addSortField(SortField sortField) {
        if (sortField.getType() == SortField.Type.INT) {
            sortField = new SortField(sortField.getField(), SortField.Type.LONG, sortField.getReverse());
        } else if (sortField.getType() == SortField.Type.FLOAT) {
            sortField = new SortField(sortField.getField(), SortField.Type.DOUBLE, sortField.getReverse());
        }
//		if (sortField.getType() == SortField.Type.DOUBLE && sortField.getField().indexOf("#double") == -1) {
//			sortField=new SortField(sortField.getField() + "#double", sortField.getType(), sortField.getReverse());
//		} 
        sortFields.add(sortField);
        return this;
    }

    public Multimap<String, Object> getMustParam() {
        return mustParam;
    }

    public Multimap<String, Object> getIncludeParam() {
        return includeParam;
    }

    public Multimap<String, Object> getExcludeParam() {
        return excludeParam;
    }

    public List<SearchParam> getMustList() {
        return mustList;
    }

    public List<SearchParam> getIncludeList() {
        return includeList;
    }

    public List<SearchParam> getExcludeList() {
        return excludeList;
    }

    public Map<String, QueryType> getQueryMethod() {
        return queryMethod;
    }

    public Map<String, boolean[]> getQueryRange() {
        return queryRange;
    }

    public int getStartIndex() {
        return this.start;
    }

    public int getPageSize() {
        return this.pageSize;
    }

    public SortField[] getSortFields() {
        return sortFields.toArray(new SortField[0]);
    }

}
