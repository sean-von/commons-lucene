package com.smikevon.lucene.index;

import java.math.BigDecimal;
import java.util.Date;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexableField;

/**
 * 对单一文档进行操作的类
 * @author huangbin
 */
public class DocParam {
    protected Document doc = new Document();

    public DocParam() {
    }

    public static boolean isDouble(Number value) {
        if (value.getClass().equals(Float.class) || value.getClass().equals(Double.class)
                || value.getClass().equals(BigDecimal.class)) {
            return true;
        }
        return false;
    }

    /**
     * 以参数type来决定索引、保存的方式
     * @param propName
     * @param value
     * @param type
     */
    public void add(String propName, String value, FieldType type) {
        if (value == null) {
            return;
        }
        this.doc.add(new Field(propName, value, type));
    }

    /**
     * 索引且保存字符串
     * @param propName
     * @param value
     */
    public void add(String propName, String value) {
        if (value == null) {
            return;
        }
        this.doc.add(new StringField(propName, value, Field.Store.YES));
    }

    /**
     * 索引且保存字符串
     * @param propName
     * @param value
     */
    public void add(String propName, String value, boolean tokenized) {
        if (value == null) {
            return;
        }
        FieldType type = new FieldType();
        type.setIndexed(true);
        type.setOmitNorms(true);
        type.setIndexOptions(IndexOptions.DOCS_ONLY);
        type.setStored(true);
        type.setTokenized(tokenized);
        type.freeze();
        this.doc.add(new Field(propName, value, type));
    }

    /**
     * 索引且保存数字
     * @param propName
     * @param value
     */
    public void add(String propName, Number value) {
        if (value == null) {
            return;
        }
        if (isDouble(value)) {
//			FieldType store = new FieldType();
//			store.setIndexed(false);
//			store.setTokenized(true);
//			store.setOmitNorms(true);
//			store.setIndexOptions(IndexOptions.DOCS_ONLY);
//			store.setNumericType(FieldType.NumericType.DOUBLE);
//			store.setStored(true);
//			store.freeze();
//			this.doc.add(new DoubleField(propName, value.doubleValue(), store));
//
//			FieldType index = new FieldType();
//			index.setIndexed(true);
//			index.setTokenized(true);
//			index.setOmitNorms(true);
//			index.setIndexOptions(IndexOptions.DOCS_ONLY);
//			index.setNumericType(FieldType.NumericType.DOUBLE);
//			index.freeze();
//			this.doc.add(new DoubleField(propName+"#double", value.doubleValue(), index));

            this.doc.add(new DoubleField(propName, value.doubleValue(), Field.Store.YES));
        } else {
            this.doc.add(new LongField(propName, value.longValue(), Field.Store.YES));
        }
    }

    /**
     * 索引且保存日期
     * @param propName
     * @param value
     */
    public void add(String propName, Date value) {
        if (value == null) {
            return;
        }
        this.doc.add(new LongField(propName, value.getTime(), Field.Store.YES));
    }

    /**
     * 索引但不保存字符串
     * @param propName
     * @param value
     */
    public void index(String propName, String value) {
        if (value == null) {
            return;
        }
        this.doc.add(new StringField(propName, value, Field.Store.NO));
    }

    /**
     * 索引但不保存字符串
     * @param propName
     * @param value
     */
    public void index(String propName, String value, boolean tokenized) {
        if (value == null) {
            return;
        }
        FieldType index = new FieldType();
        index.setIndexed(true);
        index.setOmitNorms(true);
        index.setIndexOptions(IndexOptions.DOCS_ONLY);
        index.setTokenized(tokenized);
        index.freeze();
        this.doc.add(new Field(propName, value, index));
    }

    /**
     * 索引但不保存数字
     * @param propName
     * @param value
     */
    public void index(String propName, Number value) {
        if (value == null) {
            return;
        }
        if (isDouble(value)) {
//			FieldType index = new FieldType();
//			index.setIndexed(true);
//			index.setTokenized(true);
//			index.setOmitNorms(true);
//			index.setIndexOptions(IndexOptions.DOCS_ONLY);
//			index.setNumericType(FieldType.NumericType.DOUBLE);
//			index.freeze();
//			this.doc.add(new DoubleField(propName+"#double", value.doubleValue(), index));

            this.doc.add(new DoubleField(propName, value.doubleValue(), Field.Store.NO));
        } else {
            this.doc.add(new LongField(propName, value.longValue(), Field.Store.NO));
        }
    }

    /**
     * 索引但不保存日期
     * @param propName
     * @param value
     */
    public void index(String propName, Date value) {
        if (value == null) {
            return;
        }
        this.doc.add(new LongField(propName, value.getTime(), Field.Store.NO));
    }

    /**
     * 不索引但保存字符串
     * @param propName
     * @param value
     */
    public void store(String propName, String value) {
        FieldType type = new FieldType();
        type.setIndexed(false);
        type.setOmitNorms(true);
        type.setIndexOptions(IndexOptions.DOCS_ONLY);
        type.setStored(true);
        type.setTokenized(false);
        type.freeze();
        add(propName, value, type);
    }

    public void addField(IndexableField field) {
        getDocument().add(field);
    }

    public Document getDocument() {
        return this.doc;
    }
}
