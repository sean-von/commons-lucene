package com.smikevon.lucene.search;

import java.math.BigDecimal;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

import com.smikevon.lucene.search.SearchParam.LogicType;
import com.smikevon.lucene.search.SearchParam.QueryType;

/**
 *
 * 封装了查询索引方法的类
 *
 * <p>查询方法优先级:and>or>not 都调用了and、or、not方法（用A、O、N表示），逻辑体现为： <br>
 * ((A and A and …) or O or O or …) and not N and not N …
 * </p>
 *
 * <p>构造复杂查询条件 (O or O …) and A and A… 可以用下面的方式实现：<br>
 * q.and(new SimpleQuery().or(“f1”,”v1”).or(“f2”,”v2”)) .and(“f3”,”v3”).and(“f4”,”v4”);</p>
 *
 * @author huangbin
 *
 */
public class SimpleQuery {
    private AbstractLuceneSearch searcher;
    private SearchParam param = new SearchParam();

    /**
     * 如果用该构造方法，而又没有调用index方法，调用get、getAll、getTotalCount将会报错
     */
    public SimpleQuery() {

    }

    /**
     *
     * @param indexPath
     *            索引文件位置
     */
    public SimpleQuery(String indexPath) {
        searcher = new AbstractLuceneSearch(indexPath) {
        };
    }

    /**
     * 当以无参构造函数构造时，需调用此方法确定索引位置，之后才能调用get、getAll、getTotalCount
     * @param indexPath 索引文件位置
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery index(String indexPath) {
        searcher = new AbstractLuceneSearch(indexPath) {
        };
        return this;
    }

    /**
     * 若索引已经更新，调用此方法可以查到更新后的内容
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery needReopen() {
        searcher.needReopen();
        return this;
    }

    /**
     * 与(and)方式增加查询条件
     *
     * @param fieldName 查询属性名
     * @param value 查询属性值,不能为空指针否则不起作用
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery and(String fieldName, String value) {
        if (value != null)
            param.addParam(fieldName, value, LogicType.AND);
        return this;
    }

    /**
     * 与(and)方式增加查询条件，与and方法的区别在于若参数为空字符串则会忽略掉
     * @param fieldName  查询属性名
     * @param value 查询属性值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery andIfNotEmpty(String fieldName, String value) {
        if (StringUtils.isNotEmpty(value)) {
            return this.and(fieldName, value);
        } else {
            return this;
        }
    }

    /**
     * 与(and)方式增加查询条件
     *
     * @param fieldName 查询属性名
     * @param value 查询属性值，不能为空指针否则不起作用
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery and(String fieldName, Long value) {
        if (value != null)
            param.addParam(fieldName, value, value, LogicType.AND, true, true);
        return this;
    }

    /**
     * 与(and)方式增加查询条件
     *
     * @param fieldName 查询属性名
     * @param value 查询属性值，不能为空指针否则不起作用
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery and(String fieldName, Double value) {
        if (value != null)
            param.addParam(fieldName, value, value, LogicType.AND, true, true);
        return this;
    }

    /**
     * 与(and)方式增加查询条件
     *
     * @param fieldName 查询属性名
     * @param value 查询属性值，不能为空指针否则不起作用
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery and(String fieldName, Date value) {
        if (value != null)
            param.addParam(fieldName, value.getTime(), value.getTime(), LogicType.AND, true, true);
        return this;
    }

    /**
     * 与(and)方式增加查询条件
     * @param query 包含查询条件的SimpleQuery对象
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery and(SimpleQuery query) {
        if (query != null)
            param.addParam(query.param, LogicType.AND);
        return this;
    }

    /**
     * 或(or)方式增加查询条件
     *
     * @param fieldName 查询属性名
     * @param value 查询属性值,不能为空指针否则不起作用,且若其元素为空指针则该元素不起作用
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery or(String fieldName, String... value) {
        if (value != null) {
            for (String val : value) {
                if (val != null)
                    param.addParam(fieldName, val, LogicType.OR);
            }
        }
        return this;
    }

    /**
     * 或(or)方式增加查询条件，与or方法的区别在于若参数为空字符串则会忽略掉
     * @param fieldName 查询属性名
     * @param value 查询属性值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery orIfNotEmpty(String fieldName, String... value) {
        if (value != null)
            for (String val : value) {
                if (StringUtils.isNotEmpty(val))
                    param.addParam(fieldName, val, LogicType.OR);
            }
        return this;
    }

    /**
     * 或(or)方式增加查询条件
     * @param fieldName 查询属性名
     * @param value 查询属性值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery or(String fieldName, Long... value) {
        if (value != null) {
            for (Long val : value) {
                if (val != null) {
                    param.addParam(fieldName, val, val, LogicType.OR, true, true);
                }
            }
        }
        return this;
    }

    /**
     * 或(or)方式增加查询条件
     * @param fieldName 查询属性名
     * @param value 查询属性值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery or(String fieldName, Double... value) {
        if (value != null) {
            for (Double val : value) {
                if (val != null) {
                    param.addParam(fieldName, val, val, LogicType.OR, true, true);
                }
            }
        }
        return this;
    }

    /**
     * 或(or)方式增加查询条件
     * @param fieldName 查询属性名
     * @param value 查询属性值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery or(String fieldName, Date... value) {
        if (value != null) {
            for (Date val : value) {
                if (val != null)
                    param.addParam(fieldName, val.getTime(), val.getTime(), LogicType.OR, true, true);
            }
        }
        return this;
    }

    /**
     * 或(or)方式增加查询条件
     * @param query 包含查询条件的SimpleQuery对象
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery or(SimpleQuery query) {
        if (query != null)
            param.addParam(query.param, LogicType.OR);
        return this;
    }

    /**
     * 非(not)方式增加查询条件
     *
     * @param fieldName 查询属性名
     * @param value 查询属性值,不能为空指针否则不起作用,且若其元素为空指针则该元素不起作用
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery not(String fieldName, String... value) {
        if (value != null) {
            for (String val : value) {
                if (val != null)
                    param.addParam(fieldName, val, LogicType.NOT);
            }
        }
        return this;
    }

    /**
     * 非(not)方式增加查询条件，与or方法的区别在于若参数为空字符串则会忽略掉
     * @param fieldName 查询属性名
     * @param value 查询属性值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery notIfNotEmpty(String fieldName, String... value) {
        if (value != null)
            for (String val : value) {
                if (StringUtils.isNotEmpty(val))
                    param.addParam(fieldName, val, LogicType.NOT);
            }
        return this;
    }

    /**
     * 非(not)方式增加查询条件
     * @param fieldName 查询属性名
     * @param value 查询属性值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery not(String fieldName, Long... value) {
        if (value != null) {
            for (Long val : value) {
                if (val != null)
                    param.addParam(fieldName, val, val, LogicType.NOT, true, true);
            }
        }
        return this;
    }

    /**
     * 非(not)方式增加查询条件
     * @param fieldName 查询属性名
     * @param value 查询属性值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery not(String fieldName, Double... value) {
        if (value != null) {
            for (Double val : value) {
                if (val != null)
                    param.addParam(fieldName, val, val, LogicType.NOT, true, true);
            }
        }
        return this;
    }

    /**
     * 非(not)方式增加查询条件
     * @param fieldName 查询属性名
     * @param value 查询属性值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery not(String fieldName, Date... value) {
        if (value != null) {
            for (Date val : value) {
                if (val != null)
                    param.addParam(fieldName, val.getTime(), val.getTime(), LogicType.NOT, true, true);
            }
        }
        return this;
    }

    /**
     * 非(not)方式增加查询条件
     * @param query 包含查询条件的SimpleQuery对象
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery not(SimpleQuery query) {
        if (query != null)
            param.addParam(query.param, LogicType.NOT);
        return this;
    }

    /**
     * 增加存在参数field的查询条件
     *
     * @param fieldName 查询排除的属性名
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery andExistsField(String fieldName) {
        return and(fieldName, "*");
    }

    /**
     * 增加不存在参数field的查询条件
     *
     * @param fieldName 查询排除的属性名
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery notExistsField(String fieldName) {
        return not(fieldName, "*");
    }

    /**
     * 增加查询条件：由logicType决定采用与或非哪种方式添加查询,若参数为空字符串则会忽略掉
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery condition(SimpleQuery query,LogicType logicType) {
        if (query != null)
            param.addParam(query.param, logicType);
        return this;
    }

    /**
     * 增加查询条件：由logicType决定采用与或非哪种方式添加查询,若参数为空字符串则会忽略掉
     * @param fieldName  查询属性名
     * @param value 查询属性值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery condition(String fieldName, QueryType queryType, LogicType logicType, String... value) {
        if (value != null) {
            for (String val : value) {
                if (val != null)
                    param.addParam(fieldName, val, queryType, logicType);
            }
        }
        return this;
    }

    /**
     * 增加查询条件：由logicType决定采用与或非哪种方式添加查询,若参数为空字符串则会忽略掉
     * @param fieldName  查询属性名
     * @param value 查询属性值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery analyzed(String fieldName, String value, LogicType logicType) {
        return condition(fieldName, QueryType.ANALYZED, logicType, value);
    }

    /**
     * 增加查询条件：由logicType决定采用与或非哪种方式添加查询,若参数为空字符串则会忽略掉
     * @param fieldName  查询属性名
     * @param value 查询属性值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery fuzzy(String fieldName, String value, LogicType logicType) {
        return condition(fieldName, QueryType.FUZZY, logicType, value);
    }

    /**
     * 增加查询条件：由logicType决定采用与或非哪种方式添加查询,若参数为空字符串则会忽略掉
     * @param fieldName  查询属性名
     * @param value 查询属性值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery prefix(String fieldName, String value, LogicType logicType) {
        return condition(fieldName, QueryType.PREFIX, logicType, value);
    }

    /**
     * 增加查询条件：由logicType决定采用与或非哪种方式添加查询,若参数为空字符串则会忽略掉
     * @param fieldName  查询属性名
     * @param value 查询属性值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery regexp(String fieldName, String value, LogicType logicType) {
        return condition(fieldName, QueryType.REGEXP, logicType, value);
    }

    /**
     * 增加查询条件：由logicType决定采用与或非哪种方式添加查询,若参数为空字符串则会忽略掉
     * @param fieldName  查询属性名
     * @param value 查询属性值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery wildcard(String fieldName, String value, LogicType logicType) {
        return condition(fieldName, QueryType.WILDCARD, logicType, value);
    }

    /**
     * 增加查询条件: 由logicType决定采用与或非哪种方式添加查询
     * @param fieldName 查询属性名
     * @param start 查询属性起始值
     * @param end 查询属性结束值
     * @param logicType 查询逻辑
     * @param includeMin 是否包含起始值
     * @param includeMax 是否包含结束值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery range(String fieldName, String start, String end, LogicType logicType, boolean includeMin,
                             boolean includeMax) {
        if (start != null || end != null)
            param.addParam(fieldName, start, end, logicType, includeMin, includeMax);
        return this;
    }

//	/**
//	 * 增加查询条件: 由logicType决定与或非哪种方式添加查询
//	 * @param fieldName 查询属性名
//	 * @param start 查询属性起始值
//	 * @param end 查询属性结束值
//	 * @param logicType 查询逻辑
//	 * @param includeMin 是否包含起始值
//	 * @param includeMax 是否包含结束值
//	 * @return 链式返回SimpleQuery对象
//	 */
//	public SimpleQuery range(String fieldName, Long start, Long end, LogicType logicType, boolean includeMin,
//			boolean includeMax) {
//		if (start != null && end != null)
//			param.addParam(fieldName, start, end, logicType, includeMin, includeMax);
//		return this;
//	}
//
//	/**
//	 * 增加查询条件: 由logicType决定与或非哪种方式添加查询
//	 * @param fieldName 查询属性名
//	 * @param start 查询属性起始值
//	 * @param end 查询属性结束值
//	 * @param logicType 查询逻辑
//	 * @param includeMin 是否包含起始值
//	 * @param includeMax 是否包含结束值
//	 * @return 链式返回SimpleQuery对象
//	 */
//	public SimpleQuery range(String fieldName, Double start, Double end, LogicType logicType, boolean includeMin,
//			boolean includeMax) {
//		if (start != null && end != null)
//			param.addParam(fieldName, start, end, logicType, includeMin, includeMax);
//		return this;
//	}
    /**
     * 增加查询条件: 由logicType决定与或非哪种方式添加查询
     * @param fieldName 查询属性名
     * @param start 查询属性起始值
     * @param end 查询属性结束值
     * @param logicType 查询逻辑
     * @param includeMin 是否包含起始值
     * @param includeMax 是否包含结束值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery rangeLong(String fieldName, Number start, Number end, LogicType logicType, boolean includeMin,
                                 boolean includeMax) {
        if (start != null || end != null) {
            boolean include1 = includeMin;
            Long value1 = null;
            if (start != null) {
                BigDecimal beginSrc = new BigDecimal(start.toString());
                BigDecimal beginScale = beginSrc.setScale(0, BigDecimal.ROUND_CEILING);
                value1 = beginScale.longValue();
                if (beginScale.compareTo(beginSrc) > 0) {
                    include1 = true;
                } else {
                    include1 = includeMin;
                }
            }

            boolean include2 = includeMax;
            Long value2 = null;
            if (end != null) {
                BigDecimal endSrc = new BigDecimal(end.toString());
                value2 = end.longValue();
                if (endSrc.compareTo(new BigDecimal(value2)) > 0) {
                    include2 = true;
                } else {
                    include2 = includeMax;
                }
            }
            param.addParam(fieldName, value1, value2, logicType, include1, include2);
        }
        return this;
    }

    /**
     * 增加查询条件: 由logicType决定与或非哪种方式添加查询
     * @param fieldName 查询属性名
     * @param start 查询属性起始值
     * @param end 查询属性结束值
     * @param logicType 查询逻辑
     * @param includeMin 是否包含起始值
     * @param includeMax 是否包含结束值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery rangeDouble(String fieldName, Number start, Number end, LogicType logicType, boolean includeMin,
                                   boolean includeMax) {
        if (start != null || end != null) {
            Double value1 = null;
            if (start != null) {
                value1 = start.doubleValue();
            }
            Double value2 = null;
            if (end != null) {
                value2 = end.doubleValue();
            }
            param.addParam(fieldName, value1, value2, logicType, includeMin, includeMax);
        }
        return this;
    }

    /**
     * 增加查询条件: 由logicType决定与或非哪种方式添加查询
     * @param fieldName 查询属性名
     * @param start 查询属性起始值
     * @param end 查询属性结束值
     * @param logicType 查询逻辑
     * @param includeMin 是否包含起始值
     * @param includeMax 是否包含结束值
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery range(String fieldName, Date start, Date end, LogicType logicType, boolean includeMin,
                             boolean includeMax) {
        if (start != null || end != null) {
            Long value1 = null;
            if (start != null) {
                value1 = start.getTime();
            }
            Long value2 = null;
            if (end != null) {
                value2 = end.getTime();
            }

            param.addParam(fieldName, value1, value2, logicType, includeMin, includeMax);
        }
        return this;
    }

    /**
     *  添加排序字段
     * @param sortField  排序属性
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery addSortField(SortField sortField) {
        param.addSortField(sortField);
        return this;
    }

    /**
     * 获取指定起止范围的结果集
     * @param start 开始位置
     * @param size 结果集最大数
     * @return 指定起止范围的结果集
     */
    public SearchResult get(int start, int size) {
        checkSearcher();
        param.setSearchRange(start, size);
        return searcher.query(param);
    }

    /**
     * 获取所有的结果集
     * @return 符合查询条件的所有的结果集
     */
    public SearchResult getAll() {
        checkSearcher();
        param.setSearchRange(0, (int) getTotalCount());
        return searcher.query(param);
    }

    /**
     * 返回符合查询条件的结果总数
     * @return 符合查询条件的结果总数
     */
    public int getTotalCount() {
        checkSearcher();
        param.setSearchRange(0, 0);
        SearchResult result = searcher.query(param);
        return result.getTotalCount();
    }

    public Query getQuery() throws ParseException {
        return searcher.getQuery(param);
    }

    /**
     * 清除已设置的参数
     * @return 链式返回SimpleQuery对象
     */
    public SimpleQuery clearParam() {
        param = new SearchParam();
        return this;
    }

    private void checkSearcher() {
        if (searcher == null) {
            throw new LuceneSearchException("indexPath has not been set!");
        }
    }

}
