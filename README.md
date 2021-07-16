# Overview
Backup and Restore (BR) is a CommandLine Interface Tool to back up data of graph spaces of [Nebula Graph](https://github.com/vesoft-inc/nebula-graph) and to restore data from the backup files.

# Features
- Full backup or restore in one-click operation
- Supported multiple backend types for storing the backup files:
  - Local Disk
  - Hadoop HDFS
  - Alibaba Cloud OSS
  - Amazon S3 (_EXPERIMENTAL_)
- Supports backing up data of entire Nebula Graph cluster or specified spaces of it（_EXPERIMENTAL_）

# Limitation
- Incremental backup not supported for now
- Nebula Listeners is not backuped for now
- Restore operation is performed OFFLINE
- During backup process, DDL and DML operation would be blocked
- For backup to local disk, backup files would be placed at each services(e.g. storage or meta)'s local path. A recommended practice is to mount a NFS Filesystem at that path so that one can restore the backup files to a difference host. For detail, please reference to the [Implementation](#Implementation) part.
- Restoring a backup of specified spaces is only allowed to perform INPLACE, which means that if one backup a specified space from Cluster-A, this backup cannot be restored to another cluster(Let's say Cluster-B). Restoring an entire backup wouldn't have this limitation
- Target cluster to restore must have the same topologies with the cluster where the backup comes from
- Hosts where BR CLI run and the hosts of target cluster(both storage and meta service) be authenticated with provided username in SSH Tunnel protocol

# Prerequisites
- Hosts of cluster and host of CLI running at should be ssh authenticated with provided username. 
- Hosts of cluster has installed cli tools of selected backend in $PATH: `hadoop` for HDFS, `ossutil` for Alibaba Cloud OSS, `aws` for amazon s3, etc.

# Quick Start
- Clone the tool repo: 
```
git clone https://github.com/vesoft-inc/nebula-br.git
```

- Compile the tool:
```
make
```

- Test the compiled binary file:
```
bin/br version
```

- Basically one can run with `--help` for each subcommand usage of BR.
  - Backup a cluster:
  ```
  Usage:
    br backup full [flags]

  Flags:
    -h, --help   help for full

  Global Flags:
        --concurrent int       max concurrent(for aliyun OSS) (default 5)
        --connection int       max ssh connection (default 5)
        --extra_args string    backup storage utils(oss/hdfs/s3) args for backup
        --log string           log path (default "br.log")
        --meta string          meta server
        --spaces stringArray   (EXPERIMENTAL)space names.
                                   By this option, user can specify which spaces to backup. Now this feature is still experimental.

        --storage string       backup target url, format: <SCHEME>://<PATH>.
                                   <SCHEME>: a string indicating which backend type. optional: local, hdfs.
                                   now hdfs and local is supported, s3 and oss are still experimental.
                                   example:
                                   for local - "local:///the/local/path/to/backup"
                                   for hdfs  - "hdfs://example_host:example_port/examplepath"
                                   for oss - "oss://example/url/to/the/backup"
                                   (EXPERIMENTAL) for s3  - "s3://example/url/to/the/backup"

        --user string          username to login into the hosts where meta/storage service located
        --verbose              show backup detailed informations
  ```

  For example, the command below will conduct a full backup operation of entire cluster whose meta service's address is `0.0.0.0:1234`, with username `foo` to ssh-login hosts of cluster and upload the backup files to HDFS URL `hdfs://0.0.0.0:9000/example/backup/path`.
  ```
  br backup full --meta "0.0.0.0:1234" --storage "hdfs://0.0.0.0:9000/example/backup/path" --user "foo" --verbose
  ```

  - Show information of existing backups:
  ```
  Usage:
    br show [flags]

  Flags:
    -h, --help             help for show
        --storage string   storage path

  Global Flags:
        --log string   log path (default "br.log")
  ```

  For example, the command below will list the information of existing backups in HDFS URL `hdfs://0.0.0.0:9000/example/backup/path`
  ```
  br show --storage "hdfs://0.0.0.0:9000/example/backup/path"
  ```

  Output of `show` subcommand would be like below:
  ```
  +----------------------------+---------------------+------------------------------------+-------------+--------------+
  |            NAME            |     CREATE TIME     |               SPACES               | FULL BACKUP | SYSTEM SPACE |
  +----------------------------+---------------------+------------------------------------+-------------+--------------+
  | BACKUP_2021_07_16_02_39_04 | 2021-07-16 10:39:05 | basketballplayer                   | true        | true         |
  +----------------------------+---------------------+------------------------------------+-------------+--------------+
  ```


  - Restore cluster from a specified backup:
  ```
  Usage:
    br restore [command]

  Available Commands:
    full        full restore Nebula Graph Database

  Flags:
        --concurrent int      max concurrent(for aliyun OSS) (default 5)
        --extra_args string   storage utils(oss/hdfs/s3) args for restore
    -h, --help                help for restore
        --meta string         meta server
        --name string         backup name
        --storage string      storage path
        --user string         user for meta and storage

  Global Flags:
        --log string   log path (default "br.log")

  Use "br restore [command] --help" for more information about a command.
  ```

  For example, the command below will conduct a restore operation, which restore to the cluster whose meta service address is `0.0.0.0:1234`, from local disk in path `/example/backup/path`. 
  Note that by local disk backend, it will restore the backup files from the local path of the target cluster. If target cluster's host has changed, it may encounter an error because of missing files. A recommend practice is to mount a common NFS to prevent that. 
    ```
    br restore full --meta "0.0.0.0:1234" --storage "local:///example/backup/path" --name "BACKUP_2021_07_16_02_39_04" --user "foo"
    ```

  - Clean up temporary files if any error occured during backup.
    ```
    Usage:
      br cleanup [flags]

    Flags:
          --backup_name string   backup name
      -h, --help                 help for cleanup
          --meta strings         meta server

    Global Flags:
          --log string   log path (default "br.log")
    ```

# Implementation<a name="Implementation"></a>

## Backup
 BR CLI would send an RPC request to leader of the meta services of Nebula Graph to backup the cluster. Before the backup is created, the meta service will block any writes to the cluster, including DDL and DML statements. The blocking operation is involved with the raft layer of cluster. After that, meta service send an RPC request to all storage service to create snapshot. Metadata of the cluster stored in meta services will be backup as well. Those backup files includes:
 - The backup files of storage service are snapshots of wal for raft layer and snapshots of lower-level storage engine, rocksdb's checkpoint for example. 
 - The backup files of meta service are a list of SSTables exported by scanning some particular metadatas.
 After backup files generated, a metafile which describing this backup would be generated. Along with the backup files, BR CLI would upload those files and the meta file into user specified backends. Note that for local disk backend, backup files would be copied to a local path of services defined by `--storage`, the meta file would be copied into a local path of the host where BR CLI running at.
 
## Restore
 BR CLI would first check the topologies of the target cluster and the backup. If not match the requirements, the restore operation would be abort.
 Before restore, BR CLI would stop the meta and storage service remotely. If the backup contain entire cluster, the original data of target cluster would be backup to a temporary path end up with `_old_<timestamp>` before restoring, in case of any error ocurred.
 When restoring, BR CLI would try to repick hosts for each space from target cluster and download the backup files from the specified backend to the target hosts.
 - For restoring meta service's data, BR CLI would bulkload the SSTables into meta services at first. Then update cluster metadata based on repicked hosts.
 - For restoring storage service's data, BR CLI would download the snapshots and restart storage service.

 
