Network Block Device
====================

The nbd-client on Linux allows you to mount a remote server implementing the NBD protocol
as a local block device. This NBDServer exports volumes whose data is stored in
FoundationDB. This gives you a highly scalable, high performance, reliable block device that
you can then format and put a filesystem on it.

The NBDCLI command allows you to create new volume, list the volumes you have in your system,
delete a volume or snapshot a current volume to another volume.

HOWTO
=====

Create a new 1G volume:

```bash
NBDCLI create -n [volume name] -s 1G 
```

Bring up FoundationDB and then run the NBDServer. It will be listening on the default 10809 port.

On a Linux host, install ndb, create the block device, format it and mount it:

```bash
sudo apt-get update && apt-get install nbd
sudo modprobe nbd
sudo nbd-client -N [volume name] [host] /dev/nbd0
sudo mkfs.xfs /dev/nbd0
mkdir tmp
sudo mount /dev/nbd0 tmp
```

You may need to change the ownership on that directory to access it but you can now save files
there and they will be backed by FoundationDB. Each volume can only be shared to a single nbd client
at a time. 

Under the covers
================

Each volume is a sparse array of bytes (FDBArray) stored in FoundationDB across many rows in the database. In addition
each volume can have a parent whose sparse array shows through where the child volume hasn't written yet. Here is the
interface that we implement:

```java
public interface Storage {
  void connect();

  void disconnect();

  CompletableFuture<Void> read(byte[] buffer, long offset);

  CompletableFuture<Void> write(byte[] buffer, long offset);

  CompletableFuture<Void> flush();

  long size();

  long usage();
}
```
