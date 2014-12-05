package com.smikevon.lucene.index;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockReleaseFailedException;
/**
 * 
 * @author huangbin
 *
 */
public class NFSLock extends Lock {

	  File lockFile;
	  File lockDir;

	  public NFSLock(File lockDir, String lockFileName) {
	    this.lockDir = lockDir;
	    lockFile = new File(lockDir, lockFileName);
	  }

	  @Override
	  public boolean obtain() throws IOException {

	    // Ensure that lockDir exists and is a directory:
	    if (!lockDir.exists()) {
	      if (!lockDir.mkdirs())
	        throw new IOException("Cannot create directory: " +
	                              lockDir.getAbsolutePath());
	    } else if (!lockDir.isDirectory()) {
	      throw new IOException("Found regular file where directory expected: " + 
	                            lockDir.getAbsolutePath());
	    }
	    return lockFile.createNewFile();
	  }

	  @Override
	  public void release() throws LockReleaseFailedException {
	    if (lockFile.exists() && !lockFile.delete())
	      throw new LockReleaseFailedException("failed to delete " + lockFile);
	  }

	  @Override
	  public boolean isLocked() {
	    return lockFile.exists();
	  }

	  @Override
	  public String toString() {
	    return "NFSLock@" + lockFile;
	  }
	  
	  public long getLockedTime(){
		  return lockFile.lastModified();
	  }
}