package com.smikevon.lucene.search;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
/**
 *
 * 表示查询结果的类
 * @author huangbin
 */
public class SearchResult implements Iterable<Map<String, Object>> {
	private int currentPageNo;
	private int startIndex;
	private int pageSize;
	private int totalCount;
	private List<Map<String, Object>> result;

	public SearchResult() {
		result = new ArrayList<Map<String, Object>>();
	};

	public SearchResult(int startIndex, int pageSize, int totalCount, List<Map<String, Object>> result) {
		this.startIndex = startIndex;
		if (pageSize != 0 && startIndex % pageSize == 0) {
			currentPageNo = startIndex / pageSize + 1;
		} else {
			currentPageNo = -1;
		}
		this.pageSize = pageSize;
		this.totalCount = totalCount;
		this.result = result;
	}

	public int size() {
		return result.size();
	}

	public Map<String, Object> get(int i) {
		if (i < 0)
			return null;

		if (result.size() > i)
			return result.get(i);
		else
			return null;
	}

	public List<Map<String, Object>> getResult() {
		return result;
	}

	@Override
	public Iterator<Map<String, Object>> iterator() {
		return result.iterator();
	}
	
	public int getCurrentPageNo() {
		return currentPageNo;
	}

	public int getPageSize() {
		return pageSize;
	}

	public int getTotalCount() {
		return totalCount;
	}

	public int getStartIndex() {
		return startIndex;
	}

	public int getEndIndex() {
		int endIndex = startIndex + pageSize - 1;
		return endIndex < totalCount ? endIndex : totalCount - 1;
	}
}
