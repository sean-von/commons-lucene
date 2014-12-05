package com.smikevon.lucene;

public class LuceneConfig {
    private static boolean writerCache = true;
    private static long writerMaxlocktime = 600000;
    private static int getWriterSleep = 1000;
    private static int readerReopen = 30;
    private static int schedulePagenum = 10000;

    public static boolean isWriterCache() {
        return writerCache;
    }

    public static long getWriterMaxlocktime() {
        return writerMaxlocktime;
    }

    public static int getGetWriterSleep() {
        return getWriterSleep;
    }

    /**
     * 设置writer需要缓存，应将写入lucene控制在单点上
     */
    public static void cacheWriter() {
        LuceneConfig.writerCache = true;
    }

    /**
     * 不缓存writer，适用于索引保存在NFS上，多进程抢占index锁的情况
     * @param writerMaxlocktime 当锁的时间超过该值表示的时间后可自动清除锁,单位为毫秒(ms)。
     * @param getWriterSleep 等待获取写锁的睡眠时间，单位为毫秒(ms)
     */
    public static void notCacheWriter(long writerMaxlocktime,int getWriterSleep) {
        LuceneConfig.writerCache = false;
        LuceneConfig.writerMaxlocktime = writerMaxlocktime;
        LuceneConfig.getWriterSleep = getWriterSleep;
    }

    public static int getReaderReopen() {
        return readerReopen;
    }

    /**
     * 设置reader可能需要重新打开的检测时间
     * @param readerReopen 单位为秒(s)
     */
    public static void setReaderReopen(int readerReopen) {
        LuceneConfig.readerReopen = readerReopen;
    }

    public static int getSchedulePagenum() {
        return schedulePagenum;
    }

    /**
     * 设置AbstractLuceneScheduleIndex类的每页记录数
     * @param schedulePagenum AbstractLuceneScheduleIndex类的每页记录数
     */
    public static void setSchedulePagenum(int schedulePagenum) {
        LuceneConfig.schedulePagenum = schedulePagenum;
    }

}
