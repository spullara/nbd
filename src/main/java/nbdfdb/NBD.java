/*
 * Copyright 2018 Sam Pullara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package nbdfdb;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public class NBD {
  public static final int NBD_OK              = 0       ; /* OK */
  public static final byte[] NBD_OK_BYTES     = new byte[4];

  public static final int NBD_FLAG_HAS_FLAGS  = (1 << 0);	/* Flags are there */
  public static final int NBD_FLAG_READ_ONLY  =	(1 << 1);	/* Device is read-only */
  public static final int NBD_FLAG_SEND_FLUSH =	(1 << 2);	/* Send FLUSH */
  public static final int NBD_FLAG_SEND_FUA	  = (1 << 3);	/* Send FUA (Force Unit Access) */
  public static final int NBD_FLAG_ROTATIONAL =	(1 << 4);	/* Use elevator algorithm - rotational media */
  public static final int NBD_FLAG_SEND_TRIM	= (1 << 5);	/* Send TRIM (discard) */

  public static final int NBD_REQUEST_MAGIC = 0x25609513;
  public static final byte[] NBD_REQUEST_MAGIC_BYTES = Ints.toByteArray(NBD_REQUEST_MAGIC);
  public static final int NBD_REPLY_MAGIC = 0x67446698;
  public static final byte[] NBD_REPLY_MAGIC_BYTES = Ints.toByteArray(NBD_REPLY_MAGIC);

  public static final byte[] INIT_PASSWD = "NBDMAGIC".getBytes();

  public static final long CLISERV_MAGIC = 0x00420281861253L;
  public static final byte[] CLISERV_MAGIC_BYTES = Longs.toByteArray(CLISERV_MAGIC);
  public static final long OPTS_MAGIC = 0x49484156454F5054L;
  public static final byte[] OPTS_MAGIC_BYTES = Longs.toByteArray(OPTS_MAGIC);
  public static final long REP_MAGIC = 0x3e889045565a9L;
  public static final byte[] REP_MAGIC_BYTES = Longs.toByteArray(REP_MAGIC);
  public static final byte[] EMPTY_124 = new byte[124];

  // FDB Keys
  public static final byte[] SIZE_KEY = "size".getBytes();
  public static final byte[] LEASE_KEY = "lease".getBytes();

  enum Command {
    READ,
    WRITE,
    DISCONNECT,
    FLUSH,
    TRIM,
    CACHE
  }

  public static final int NBD_OPT_EXPORT_NAME = 1;
  public static final int NBD_OPT_ABORT = 2;
  public static final int NBD_OPT_LIST = 3;
}
