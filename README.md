# Overview

Backup and Restore (BR) is a CommandLine Interface Tool to back up data of graph spaces of [Nebula](https://github.com/vesoft-inc/nebula) and restore data from the backup files.

# Features

- Full backup or restore in one-click operation
- Supported multiple backend types for storing the backup files:
  - Local Disk
  - S3-Compatible Storage(such as Alibaba Cloud OSS, Amazon S3, MinIO, Ceph RGW, and so on).
- Supports backing up data of entire Nebula Graph cluster or specified spaces of it（_EXPERIMENTAL_), but now it has some limitations:
  - when restore use this, all other spaces will be erased!

# Limitation

- Incremental backup is not supported for now
- Nebula Listeners is not backed up for now
- Restore operation is performed OFFLINE
- During backup process, DDL and DML operation would be blocked
- For backup to local disk, backup files would be placed at each service(e.g. storage or meta)'s local path. A recommended practice is to mount a NFS Filesystem at that path so that one can restore the backup files to a difference host. For details, please reference to the [Implementation](#Implementation) part.
- Restoring a backup of specified spaces is only allowed to perform INPLACE, which means that if one backup a specified space from Cluster-A, this backup cannot be restored to another cluster(Let's say Cluster-B). Restoring an entire backup wouldn't have this limitation
- The target cluster to restore must have the same topologies with the cluster where the backup comes from

# Prerequisites

## Nebula Agent

Nebula cluster to back up/restore should start the [agent](https://github.com/vesoft-inc/nebula-agent) service in each cluster(including metad, storaged, graphd) host. Notice that, if you have multi-services in the same host, you need only start one agent. That is to say,  you need exactly one agent in each cluster host no matter how many services in it.
In the future, the nebula-agent will be started automatically, but now, you should start it yourself in your cluster machines one by one. You could download it from nebula-agent repo and start it as the guidance in that repo. 


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
  - Full backup a cluster:
  ```
  Usage:
    br backup full [flags]

  Flags:
    -h, --help   help for full

  Global Flags:
        --log string             Specify br detail log path (default "br.log")
        --meta string            Specify meta server, any metad server will be ok
        --spaces stringArray     (EXPERIMENTAL)space names.
                                   By this option, user can specify which spaces to backup. Now this feature is still experimental.
                                   If not specified, will backup all spaces.

        --storage string         backup target url, format: <SCHEME>://<PATH>.
                                     <SCHEME>: a string indicating which backend type. optional: local, s3.
                                     now only s3-compatible backend is supported.
                                     example:
                                     for local - "local:///the/local/path/to/backup"
                                     for s3  - "s3://example/url/to/the/backup" 

        --s3.access_key string   S3 Option: set access key id
        --s3.endpoint string     S3 Option: set the S3 endpoint URL, please specify the http or https scheme explicitly
        --s3.region string       S3 Option: set region or location to upload or download backup
        --s3.secret_key string   S3 Option: set secret key for access id
        --debug                  Output log in debug level or not
  ```

  For example, the command below will conduct a full backup operation of entire cluster whose meta service's address is `127.0.0.1:9559`, upload the backup files to local `/home/nebula/backup`  or S3 URL `s3://127.0.0.1:9000/br-test/backup`.
  ```bash
  # for local
  br backup full --meta "127.0.0.1:9559" --storage "local:///home/nebula/backup/"
  # for s3
  br backup full --meta "127.0.0.1:9559" --s3.endpoint "http://127.0.0.1:9000" --storage="s3://br-test/backup/" --s3.access_key=minioadmin --s3.secret_key=minioadmin --s3.region=default
  ```

  Note: only when the storage uri is "s3://xxx", the s3 option is necessary. If the uri is "local://xxx", the s3 option is useless.

  - Show information of existing backups:
  ```
  Usage:
  br show [flags]

  Flags:
    -h, --help                   help for show
        --log string             Specify br detail log path (default "br.log")
        --s3.access_key string   S3 Option: set access key id
        --s3.endpoint string     S3 Option: set the S3 endpoint URL, please specify the http or https scheme explicitly
        --s3.region string       S3 Option: set region or location to upload or download backup
        --s3.secret_key string   S3 Option: set secret key for access id
        --storage string         backup target url, format: <SCHEME>://<PATH>.
                                     <SCHEME>: a string indicating which backend type. optional: local, s3.
                                     now only s3-compatible backend is supported.
                                     example:
                                     for local - "local:///the/local/path/to/backup"
                                     for s3  - "s3://example/url/to/the/backup" 
        --debug                  Output log in debug level or not
  ```

  For example, the command below will list the information of existing backups in local path `/home/nebula/backup` or  S3 URL `s3://127.0.0.1:9000/br-test/backup`
  ```bash
  # for local
  br show --storage "local:///home/nebula/backup"
  # for s3
  br show --s3.endpoint "http://127.0.0.1:9000" --storage="s3://br-test/backup/" --s3.access_key=minioadmin --s3.secret_key=minioadmin --s3.region=default
  ```

  Output of `show` subcommand would be like below:
  ```
  +----------------------------+---------------------+--------+-------------+------------+
  |            NAME            |     CREATE TIME     | SPACES | FULL BACKUP | ALL SPACES |
  +----------------------------+---------------------+--------+-------------+------------+
  | BACKUP_2021_12_11_14_40_12 | 2021-12-11 14:40:43 | nba    | true        | true       |
  | BACKUP_2021_12_13_14_18_52 | 2021-12-13 14:18:52 | nba    | true        | true       |
  | BACKUP_2021_12_13_15_06_27 | 2021-12-13 15:06:29 | nba    | true        | false      |
  | BACKUP_2021_12_21_12_01_59 | 2021-12-21 12:01:59 | nba    | true        | false      |
  +----------------------------+---------------------+--------+-------------+------------+
  ```

  - Restore cluster from a specified backup:
  ```
  Usage:
   br restore full [flags]

  Flags:
    -h, --help   help for full

  Global Flags:
        --concurrency int        Max concurrency for download data (default 5)
        --log string             Specify br detail log path (default "br.log")
        --meta string            Specify meta server, any metad server will be ok
        --name string            Specify backup name

        --storage string         backup target url, format: <SCHEME>://<PATH>.
                                     <SCHEME>: a string indicating which backend type. optional: local, s3.
                                     now only s3-compatible backend is supported.
                                     example:
                                     for local - "local:///the/local/path/to/backup"
                                     for s3  - "s3://example/url/to/the/backup" 

        --s3.access_key string   S3 Option: set access key id
        --s3.endpoint string     S3 Option: set the S3 endpoint URL, please specify the http or https scheme explicitly
        --s3.region string       S3 Option: set region or location to upload or download backup
        --s3.secret_key string   S3 Option: set secret key for access id
        --debug                  Output log in debug level or not
  ```

  For example, the command below will conduct a restore operation, which restore to the cluster whose meta service address is `127.0.0.1:9559`, from local disk in path `/home/nebula/backup/BACKUP_2021_12_08_18_38_08` or s3 URL `s3://127.0.0.1:9000/br-test/backupu/BACKUP_2021_12_08_18_38_08`

  Note that by local disk backend, it will restore the backup files from the local path of the target cluster. If target cluster's host has changed, it may encounter an error because of missing files. A recommended practice is to mount a common NFS to prevent that. 

  ```bash
  # for local
  br restore full --meta "127.0.0.1:9559" --storage "local:///home/nebula/backup/" --name BACKUP_2021_12_08_18_38_08
  # for s3
  br restore full --meta "127.0.0.1:9559" --s3.endpoint "http://127.0.0.1:9000" --storage="s3://br-test/backup/" --s3.access_key=minioadmin --s3.secret_key=minioadmin --s3.region="default" --name BACKUP_2021_12_08_18_38_08
  ```

  Note: if your new cluster hosts' ip are not all the same with the backup cluster, after restore, you should add the hosts needed in the new cluster one by one.

  - Clean up temporary files if any error occurred during backup. It will clean the files in cluster and external storage. You could also use it to clean up old backups files in external storage.
  ```
  Usage:
    br cleanup [flags]

  Flags:
    -h, --help                   help for cleanup
        --log string             Specify br detail log path (default "br.log")
        --meta string            Specify meta server, any metad service will be ok
        --name string            Specify backup name

        --storage string         backup target url, format: <SCHEME>://<PATH>.
                                     <SCHEME>: a string indicating which backend type. optional: local, s3.
                                     example:
                                     for local - "local:///the/local/path/to/backup"
                                     for s3  - "s3://example/url/to/the/backup
    
        --s3.access_key string   S3 Option: set access key id
        --s3.endpoint string     S3 Option: set the S3 endpoint URL, please specify the http or https scheme explicitly
        --s3.region string       S3 Option: set region or location to upload or download backup
        --s3.secret_key string   S3 Option: set secret key for access id
        --debug                  Output log in debug level or not
  ```

  For example:
  ```bash
  # for local
  br cleanup --meta "127.0.0.1:9559" --storage="local:///home/nebula/backup/" --name BACKUP_2021_12_08_18_38_08

  # for s3
  br cleanup --meta "127.0.0.1:9559" --s3.endpoint "http://127.0.0.1:9000" --storage="s3://br-test/backup/" --s3.access_key=minioadmin --s3.secret_key=minioadmin --name=BACKUP_2021_12_08_18_38_08
  ```

# Implementation<a name="Implementation"></a>

## Backup

 BR CLI would send an RPC request to leader of the meta services of Nebula Graph to back up the cluster. Before the backup is created, the meta service will block any writes to the cluster, including DDL and DML statements. The blocking operation is involved with the raft layer of cluster. After that, meta service send an RPC request to all storage service to create snapshot. Metadata of the cluster stored in meta services will be backup as well. Those backup files includes:
 - The backup files of storage service are snapshots of wal for raft layer and snapshots of lower-level storage engine, rocksdb's checkpoint for example. 
 - The backup files of meta service are a list of SSTables exported by scanning some particular metadata. 
 After backup files generated, a metafile which describing this backup would be generated. Along with the backup files, BR CLI would upload those files and the meta file into user specified backends. Note that for local disk backend, backup files would be copied to each local path of services defined by `--storage`, the meta file would be copied into a local path of the host where BR CLI running at. That is to say, when restoring, the BR CLI must run in the same host which it runs when backup.
 
## Restore

 BR CLI would first check the topologies of the target cluster and the backup. If not match the requirements, the restore operation would be aborted.
 Before restore, BR CLI would stop the meta and storage service remotely. If the backup contain entire cluster, the original data of target cluster would be backup to a temporary path end up with `_old_<timestamp>` before restoring, in case of any error occurred.
 When restoring, BR CLI would try to repick hosts for each space from target cluster and download the backup files from the specified backend to the target hosts.
 - For restoring meta service's data, BR CLI would bulkload the SSTables into meta services at first. Then update cluster metadata based on repicked hosts.
 - For restoring storage service's data, BR CLI would download the snapshots and restart storage service.

 
 Note: BR CLI depend on agents in cluster hosts to upload/download the backup files between the external storage and the cluster machines.

## Local Storage Mode

Local mode have strictly usage preconditions:
1. BR CLI could be only used in the same machine when backup/restore/cleanup/show all the time.
2. If you have multi-metad, you should use a shared filesystem path as the local uri which is mounted to all the cluster machines, such as nfs, distributed filesystem. Otherwise, you will restore metad service failed.

Then we suggest that you should only use local storage in experiment environment. In production environment, s3-compatible storage backend is highly recommended.

