package com.smikevon.lucene.index;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.File;
import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.LockStressTest;
import org.apache.lucene.store.LockVerifyServer;
import org.apache.lucene.store.VerifyingLockFactory;

/**
 * <p>Implements {@link org.apache.lucene.store.LockFactory} using {@link
 * java.io.File#createNewFile()}.</p>
 *
 * <p><b>NOTE:</b> the <a target="_top"
 * href="http://java.sun.com/j2se/1.4.2/docs/api/java/io/File.html#createNewFile()">javadocs
 * for <code>File.createNewFile</code></a> contain a vague
 * yet spooky warning about not using the API for file
 * locking.  This warning was added due to <a target="_top"
 * href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4676183">this
 * bug</a>, and in fact the only known problem with using
 * this API for locking is that the Lucene write lock may
 * not be released when the JVM exits abnormally.</p>

 * <p>When this happens, a {@link org.apache.lucene.store.LockObtainFailedException}
 * is hit when trying to create a writer, in which case you
 * need to explicitly clear the lock file first.  You can
 * either manually remove the file, or use the {@link
 * org.apache.lucene.index.IndexWriter#unlock(org.apache.lucene.store.Directory)}
 * API.  But, first be certain that no writer is in fact
 * writing to the index otherwise you can easily corrupt
 * your index.</p>
 *
 * <p>If you suspect that this or any other LockFactory is
 * not working properly in your environment, you can easily
 * test it by using {@link org.apache.lucene.store.VerifyingLockFactory}, {@link
 * org.apache.lucene.store.LockVerifyServer} and {@link org.apache.lucene.store.LockStressTest}.</p>
 *
 * @see org.apache.lucene.store.LockFactory
 */

public class NFSLockFactory extends FSLockFactory {

  /**
   * Create a SimpleFSLockFactory instance, with null (unset)
   * lock directory. When you pass this factory to a {@link org.apache.lucene.store.FSDirectory}
   * subclass, the lock directory is automatically set to the
   * directory itself. Be sure to create one instance for each directory
   * your create!
   */
  public NFSLockFactory() throws IOException {
    this((File) null);
  }

  /**
   * Instantiate using the provided directory (as a File instance).
   * @param lockDir where lock files should be created.
   */
  public NFSLockFactory(File lockDir) throws IOException {
    setLockDir(lockDir);
  }

  /**
   * Instantiate using the provided directory name (String).
   * @param lockDirName where lock files should be created.
   */
  public NFSLockFactory(String lockDirName) throws IOException {
    setLockDir(new File(lockDirName));
  }

  @Override
  public Lock makeLock(String lockName) {
    if (lockPrefix != null) {
      lockName = lockPrefix + "-" + lockName;
    }
    return new NFSLock(lockDir, lockName);
  }

  @Override
  public void clearLock(String lockName) throws IOException {
    if (lockDir.exists()) {
      if (lockPrefix != null) {
        lockName = lockPrefix + "-" + lockName;
      }
      File lockFile = new File(lockDir, lockName);
      if (lockFile.exists() && !lockFile.delete()) {
        throw new IOException("Cannot delete " + lockFile);
      }
    }
  }
}
