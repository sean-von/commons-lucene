package com.smikevon.lucene.search;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.smikevon.lucene.LuceneConfig;
import com.smikevon.lucene.search.SearchParam.QueryType;
/**
 *
 * 查询索引的基础抽象类
 * @author huangbin
 *
 */
public abstract class AbstractLuceneSearch {
    private static Logger log = LoggerFactory.getLogger(AbstractLuceneSearch.class);
    private String indexPath;
    //	private Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_35);
    private Analyzer analyzer = new SmartChineseAnalyzer(Version.LUCENE_43);
    private static Map<String, SearcherManager> smCache = new HashMap<String, SearcherManager>();
    private static Map<String, Long> lastReopen = new HashMap<String, Long>();

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Map<String, Object> doc2map(Document doc) {
        Map<String, Object> map = new HashMap<String, Object>();
        List fl = doc.getFields();
        for (int j = 0; j < fl.size(); j++) {
            IndexableField f = (IndexableField) fl.get(j);
            Object o;
            if ((o = map.get(f.name())) == null) {
                map.put(f.name(), f.stringValue());
            }else{
                if(o instanceof String){
                    List<String> list=new ArrayList<String>();
                    list.add((String) o);
                    list.add(f.stringValue());
                    map.put(f.name(), list);
                }else if(o instanceof List){
                    List<String> col=(List<String>)o;
                    col.add(f.stringValue());
                    map.put(f.name(), col);
                }
            }
        }
        return map;
    }

    /**
     * 此方法不需要加同步,因为SearcherManager.maybeReopen方法已经加锁
     *
     * @param indexPath
     *            秒数
     * @return 与上次reopen时间超过s秒数则返回true,否则返回false
     */
    private static boolean isNeedReopen(String indexPath) {
        Long last = lastReopen.get(indexPath);
        if (last == null) {
            lastReopen.put(indexPath, System.currentTimeMillis());
            return true;
        } else {
            long now = System.currentTimeMillis();
            if (now - last > LuceneConfig.getReaderReopen() * 1000) {
                lastReopen.put(indexPath, now);
                return true;
            } else {
                return false;
            }
        }
    }

    public void needReopen() {
        lastReopen.put(indexPath, null);
    }

    private static SearcherManager getSearcherManager(String indexPath) throws IOException {
        SearcherManager manager;
        synchronized (("smCache."+indexPath).intern()) {
            manager = smCache.get(indexPath);
            if (manager == null) {
                manager = new SearcherManager(FSDirectory.open(new File(indexPath)), null);
                smCache.put(indexPath, manager);
            }
        }
        if (isNeedReopen(indexPath)) {
            long t1 = System.nanoTime();
            manager.maybeRefresh();
            long t2 = System.nanoTime();
            log.debug("maybeReopen cost:{} ms", (t2 - t1) * 1.0 / 1000000);
        }

        return manager;
    }

    private SearchResult search(Query query, SearchParam param) {
        IndexSearcher s = null;
        SearcherManager sm = null;
        try {
            sm = getSearcherManager(indexPath);
            s = sm.acquire();

            TopDocs results = null;
            SortField[] sortArr = param.getSortFields();
            if (param.getPageSize() == 0) { //特殊状态，可用于快速查询总数
                results = s.search(query, 1);
            } else {
                if (sortArr.length > 0)
                    results = s.search(query, param.getStartIndex() + param.getPageSize(), new Sort(sortArr));
                else
                    results = s.search(query, param.getStartIndex() + param.getPageSize());
            }

            if (null == results) {
                return new SearchResult();
            }
            ScoreDoc[] hits = results.scoreDocs;
            int totalCount = results.totalHits;

            List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
            if (param.getPageSize() == 0) {
                return new SearchResult(param.getStartIndex(), param.getPageSize(), totalCount, result);
            }
            int start = param.getStartIndex();
            int end = 0;
            if (start < hits.length) {
                for (int i = 0; i < hits.length; i++) {
                    if (i < start)
                        continue;
                    Document doc = s.doc(hits[i].doc);
                    Map<String, Object> map = doc2map(doc);
                    result.add(map);
                    end = i;
                }
                log.debug("getResult from {} to {},totalCount:{}", new Object[] { start, end, totalCount });
            } else {
                log.debug("range out of result ,totalCount:{}", totalCount);
            }
            return new SearchResult(param.getStartIndex(), param.getPageSize(), totalCount, result);
        } catch (Exception e) {
            throw new LuceneSearchException(e);
        } finally {
            try {
                if (sm != null && s != null)
                    sm.release(s);
            } catch (IOException e) {
                throw new LuceneSearchException(e);
            } finally {
                s = null;
            }
        }
    }

    public AbstractLuceneSearch(String indexPath) {
        this.indexPath = indexPath;
    }

    private Query getPropQuery(SearchParam param, String propName, Object value) throws ParseException  {
        Query propQuery;
        Term term = null;
        boolean[] includes=null;
        QueryType qt = param.getQueryMethod().get(propName);
        qt = qt == null ? QueryType.WILDCARD : qt;
        switch (qt) {
            case ANALYZED:
                QueryParser qp = new QueryParser(Version.LUCENE_43, propName, analyzer);
                propQuery = qp.parse(String.valueOf(value));
                break;
            case FUZZY:
                term = new Term(propName, String.valueOf(value));
                propQuery = new FuzzyQuery(term);
                break;
            case PREFIX:
                term = new Term(propName, String.valueOf(value));
                propQuery = new PrefixQuery(term);
                break;
            case REGEXP:
                term = new Term(propName, String.valueOf(value));
                propQuery = new RegexpQuery(term);
                break;
            case TEXT_RANGE:
                String[] range = (String[]) value;
                includes=param.getQueryRange().get(propName);
                propQuery = TermRangeQuery.newStringRange(propName, range[0], range[1], includes[0], includes[1]);
                break;
//		case NUMERIC_RANGE:
//			BooleanQuery booleanQuery = new BooleanQuery();
//			
//			Number[] numRange = (Number[]) value;
//			includes=param.getQueryRange().get(propName);
//			
//			NumericRangeQuery<Double> query1 = NumericRangeQuery.newDoubleRange(propName+"#double", new BigDecimal(numRange[0].toString()).doubleValue(),
//					 new BigDecimal(numRange[1].toString()).doubleValue(), includes[0], includes[1]);
//			booleanQuery.add(query1, BooleanClause.Occur.SHOULD);
//			
//			BigDecimal beginSrc=new BigDecimal(numRange[0].toString());
//			BigDecimal beginScale=beginSrc.setScale(0, BigDecimal.ROUND_CEILING);
//			long begin = beginScale.longValue();
//			boolean includeMin;
//			if(beginScale.compareTo(beginSrc)>0){
//				includeMin=true;
//			}else{
//				includeMin=includes[0];
//			}
//			BigDecimal endSrc=new BigDecimal(numRange[1].toString());
//			long end = numRange[1].longValue();
//			boolean includeMax;
//			if(endSrc.compareTo(new BigDecimal(end))>0){
//				includeMax=true;
//			}else{
//				includeMax=includes[1];
//			}
//			
//			NumericRangeQuery<Long> query2 = NumericRangeQuery.newLongRange(propName, begin, end,
//					includeMin, includeMax);
//			booleanQuery.add(query2, BooleanClause.Occur.SHOULD);
//			
//			propQuery=booleanQuery;
//			break;
            case DOUBLE_RANGE:
                Double[] numRange = (Double[]) value;
                includes=param.getQueryRange().get(propName);

                NumericRangeQuery<Double> query1 = NumericRangeQuery.newDoubleRange(propName, numRange[0],numRange[1], includes[0], includes[1]);

                propQuery=query1;
                break;
            case LONG_RANGE:
                Long[] numRange2 = (Long[]) value;
                includes=param.getQueryRange().get(propName);

                NumericRangeQuery<Long> query2 = NumericRangeQuery.newLongRange(propName, numRange2[0], numRange2[1],includes[0], includes[1]);

                propQuery=query2;
                break;
            default:
            case WILDCARD:
                term = new Term(propName, String.valueOf(value));
                propQuery = new WildcardQuery(term);
                break;
        }
        return propQuery;
    }

    public Query getQuery(SearchParam param) throws ParseException {
        Map<String, Collection<Object>> must = param.getMustParam().asMap();
        Map<String, Collection<Object>> include = param.getIncludeParam().asMap();
        Map<String, Collection<Object>> exclude = param.getExcludeParam().asMap();

        List<SearchParam> mustList = param.getMustList();
        List<SearchParam> includeList = param.getIncludeList();
        List<SearchParam> excludeList = param.getExcludeList();

        BooleanQuery query = new BooleanQuery();

        BooleanQuery mustQuery = new BooleanQuery();
        for (Entry<String, Collection<Object>> entry : must.entrySet()) {
            for (Object value : entry.getValue()) {
                Query propQuery = getPropQuery(param, entry.getKey(), value);
                mustQuery.add(propQuery, BooleanClause.Occur.MUST);
            }
        }
        for (SearchParam sp : mustList) {
            mustQuery.add(getQuery(sp), BooleanClause.Occur.MUST);
        }
        if (must.size() > 0 || mustList.size() > 0) {
            if (include.size() > 0 || includeList.size() > 0 || exclude.size() > 0 || excludeList.size() > 0)
                query.add(mustQuery, BooleanClause.Occur.SHOULD);
            else
                return mustQuery;
        }

        for (Entry<String, Collection<Object>> entry : include.entrySet()) {
            for (Object value : entry.getValue()) {
                Query propQuery = getPropQuery(param, entry.getKey(), value);
                query.add(propQuery, BooleanClause.Occur.SHOULD);
            }
        }
        for (SearchParam sp : includeList) {
            query.add(getQuery(sp), BooleanClause.Occur.SHOULD);
        }

        if (must.size() == 0 && include.size() == 0 && mustList.size() == 0 && includeList.size() == 0) {
            query.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        }

        for (Entry<String, Collection<Object>> entry : exclude.entrySet()) {
            for (Object value : entry.getValue()) {
                Query propQuery = getPropQuery(param, entry.getKey(), value);
                query.add(propQuery, BooleanClause.Occur.MUST_NOT);
            }
        }
        for (SearchParam sp : excludeList) {
            query.add(getQuery(sp), BooleanClause.Occur.MUST_NOT);
        }

        return query;
    }

    public SearchResult query(SearchParam param) {
        SearchResult results;
        try {
            Query query = getQuery(param);
            log.debug(query.toString());
            results = search(query, param);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new LuceneSearchException(e);
        }
        return results;
    }
}
