package com.smikevon.lucene.index;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.smikevon.lucene.LuceneConfig;

/**
 * 写入索引的基础抽象类
 * @author huangbin
 */
public abstract class AbstractLuceneIndex {
    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    protected static Logger log = LoggerFactory.getLogger(AbstractLuceneIndex.class);
    protected String indexPath; // 索引全路径名
    protected final IndexWriter _writer;
    private static Map<String, IndexWriter> _writerCache = new HashMap<String, IndexWriter>();

    protected AbstractLuceneIndex(String indexPath) {
        this.indexPath = indexPath;
        this._writer = null;
    }

    /**
     * 使用该构造方法必须自行管理IndexWriter的commit和close
     *
     * @param writer
     */
    protected AbstractLuceneIndex(IndexWriter writer) {
        this._writer = writer;
    }

    public static IndexWriter getIndexWriter(String indexPath) throws LuceneIndexException {
        log.debug("getIndexWriter( {} )", indexPath);

        return getIndexWriter(indexPath, LuceneConfig.getWriterMaxlocktime());
    }

    public static IndexWriter getIndexWriter(String indexPath, Long maxlocktime) throws LuceneIndexException {
        File dirIndex = new File(indexPath);

        IndexWriter writer;
        try {
            Directory dir = FSDirectory.open(dirIndex);

            if (!LuceneConfig.isWriterCache()) {
                dir.setLockFactory(new NFSLockFactory(dirIndex)); // 将适用于索引保存在NFS的情况
            }

            // Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_35);
            Analyzer analyzer = new SmartChineseAnalyzer(Version.LUCENE_43);
            IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_43, analyzer);
            iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            // iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);

            // Optional: for better indexing performance, if you
            // are indexing many documents, increase the RAM
            // buffer. But if you do this, increase the max heap
            // size to the JVM (eg add -Xmx512m or -Xmx1g):
            //
            // iwc.setRAMBufferSizeMB(256.0);

            long t1 = System.currentTimeMillis();
            synchronized (("index." + indexPath).intern()) {
                if (LuceneConfig.isWriterCache()) {
                    if ((writer = _writerCache.get(indexPath)) != null) {
                        return writer;
                    }
                }
                if (!IndexWriter.isLocked(dir)) {
                    try {
                        writer = new IndexWriter(dir, iwc);

                        Runtime.getRuntime().addShutdownHook(new IndexWriterCloseThread(writer));
                        log.debug("after construct IndexWriter.");
                    } catch (LockObtainFailedException e) {
                        log.warn("与其它JVM的线程争抢写锁失败,将重新等待!", e);
                        long t2 = System.currentTimeMillis();
                        writer = waitGetIndexWriter(dir, iwc, System.currentTimeMillis(), maxlocktime);
                        log.debug("waitGetIndexWriter() cost {} ms.", System.currentTimeMillis() - t2);
                    }
                } else {
                    log.debug("index is lock.");
                    long t2 = System.currentTimeMillis();
                    writer = waitGetIndexWriter(dir, iwc, System.currentTimeMillis(), maxlocktime);
                    log.debug("waitGetIndexWriter() cost {} ms.", System.currentTimeMillis() - t2);
                }
                if (LuceneConfig.isWriterCache()) {
                    _writerCache.put(indexPath, writer);
                }
            }
            log.debug("getIndexWriter cost {} ms.", System.currentTimeMillis() - t1);
        } catch (Exception e) {
            throw new LuceneIndexException(e);
        }

        return writer;
    }

    private static IndexWriter waitGetIndexWriter(Directory dir, IndexWriterConfig iwc, long start, Long maxlocktime)
            throws LuceneIndexException {
        IndexWriter writer = null;
        try {
            while (true) {
                if (IndexWriter.isLocked(dir)) {
                    if (maxlocktime != null) {
                        Lock lock = dir.makeLock(IndexWriter.WRITE_LOCK_NAME);
                        if (!LuceneConfig.isWriterCache() && lock instanceof NFSLock) {
                            NFSLock nlock = (NFSLock) lock;
                            if (System.currentTimeMillis() - nlock.getLockedTime() > maxlocktime) {
                                IndexWriter.unlock(dir);
                                continue;
                            }
                        } else {
                            throw new LuceneIndexException(
                                    "when LuceneConfig.getWriterMaxlocktime() >0, LuceneConfig.isWriterCache() should not be 'true'!");
                        }
                    }
                    Thread.sleep(LuceneConfig.getGetWriterSleep());
                } else {
                    try {
                        writer = new IndexWriter(dir, iwc);

                        Runtime.getRuntime().addShutdownHook(new IndexWriterCloseThread(writer));
                    } catch (LockObtainFailedException e) {
                        log.warn("与其它JVM的线程争抢写锁失败,将重新等待!", e);
                        writer = waitGetIndexWriter(dir, iwc, start, maxlocktime);
                    }
                    break;
                }
            }
        } catch (Exception e) {
            throw new LuceneIndexException(e);
        }
        return writer;
    }

    public static void releaseIndexWriter(IndexWriter writer) throws LuceneIndexException {
        try {
            if (LuceneConfig.isWriterCache()) {
                writer.commit();
            } else {
                writer.close();
            }
        } catch (Exception e) {
            throw new LuceneIndexException(e);
        }
    }

    private void addAllIndex(int maxNumSegments) throws Exception {
        IndexWriter writer = null;
        if (_writer == null) {
            writer = getIndexWriter(indexPath);
        } else {
            writer = _writer;
        }

        if (writer != null) {
            addAllIndex(writer);

            try {
                // NOTE: if you want to maximize search performance,
                // you can optionally call forceMerge here. This can be
                // a terribly costly operation, so generally it's only
                // worth it when your index is relatively static (ie
                // you're done adding documents to it):
                //
                // writer.forceMerge(1);

                if (maxNumSegments > 0) {
                    writer.forceMerge(maxNumSegments, true);
                }

            } catch (Exception e1) {
                logger.error(e1.getMessage(), e1);
            } finally {
                if (_writer == null) {
                    if (LuceneConfig.isWriterCache())
                        writer.commit();
                    else
                        writer.close();
                }
            }
        } else {
            throw new LuceneIndexException("getIndexWriter is null!");
        }
    }

    /**
     * 设置优化的Segments数
     * @param maxNumSegments
     *            为0则不优化,>0则优化为maxNumSegments个Segments
     */
    protected final void makeIndex(int maxNumSegments) throws LuceneIndexException {
        try {
            long t1 = System.currentTimeMillis();

            addAllIndex(maxNumSegments);

            if (_writer == null)
                logger.info("===> 共计用时:{} 毫秒.", System.currentTimeMillis() - t1);
        } catch (Throwable e) {
            throw new LuceneIndexException(e);
        }

    }

    public abstract void makeIndex() throws LuceneIndexException;

    protected abstract void addAllIndex(IndexWriter writer) throws Exception;

    static class IndexWriterCloseThread extends Thread {
        private IndexWriter iw;

        public IndexWriterCloseThread(IndexWriter iw) {
            this.iw = iw;
        }

        @Override
        public void run() {
            try {
                if (iw != null)
                    iw.close();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
