// Copyright 2022 Debox Developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use futures::executor::block_on;
use futures::TryStreamExt;
use ipfs_api_backend_hyper::{Error, IpfsApi, IpfsClient, TryFromUri};
use ipfs_api_backend_hyper::request::{FilesLs, FilesRead, FilesWrite};
use ipfs_api_backend_hyper::response::{FilesEntry, FilesStatResponse};
use tokio::task::spawn_blocking;

/// Trait that defines the interface for interaction with IPFS RPC API.
#[async_trait]
pub trait PeerApi: Send + Sync + Debug {
    /// Add references to IPFS files and directories in MFS (or copy within MFS).
    async fn cp(&self, path: &String, dest: &String) -> Result<(), Error>;

    /// Flush a given path's data to disk.
    async fn flush(&self, path: &String) -> Result<(), Error>;

    /// List directories in the local mutable namespace.
    async fn ls(&self, path: &String) -> Result<Vec<PeerEntry>, Error>;

    /// Make directories.
    async fn mkdir(&self, path: &String) -> Result<PeerEntry, Error>;

    /// Move files.
    async fn mv(&self, path: &String, dest: &String) -> Result<(), Error>;

    /// Read a file in a given MFS.
    async fn read(&self, path: &String, offset: usize, count: usize) -> Result<Bytes, Error>;

    /// Remove a file.
    async fn rm(&self, path: &String) -> Result<(), Error>;

    /// Display file status.
    async fn stat(&self, path: &String) -> Result<PeerEntry, Error>;

    /// Write to a mutable file in a given filesystem.
    async fn write(
        &self, path: &String, offset: usize, truncate: bool, data: Bytes,
    ) -> Result<(), Error>;
}

/// IPFS node MFS (mutable file system) entity representation.
#[derive(Debug, Clone)]
pub struct PeerEntry {
    /// Absolute path of MFS entity.
    pub path: String,

    /// Time of MFS entity creation.
    pub crtime: SystemTime,

    /// Time of MFS entity modification.
    pub mtime: SystemTime,

    /// Whether the entity is a directory.
    pub is_dir: bool,

    /// Size of MFS entity.
    pub size: usize,
}

impl PeerEntry {
    fn new_dir(path: &String) -> Self {
        Self {
            path: path.clone(),
            crtime: SystemTime::now(),
            mtime: SystemTime::now(),
            is_dir: true,
            size: 0,
        }
    }

    fn from_stat(path: &String, stat: &FilesStatResponse) -> Self {
        Self {
            path: path.clone(),
            crtime: SystemTime::now(),
            mtime: SystemTime::now(),
            is_dir: stat.typ == "directory",
            size: stat.size as usize,
        }
    }

    fn from_entry(path: &String, entry: &FilesEntry) -> Self {
        Self {
            path: path.clone(),
            crtime: SystemTime::now(),
            mtime: SystemTime::now(),
            is_dir: entry.typ == 1,
            size: entry.size as usize,
        }
    }
}

/// The default implemented API for interfacing with the IPFS RPC API.
/// This functionality is achieved by implementing the `PeerApi` trait for `BaseApi`.
///
/// To change or enhance any of the functionality of interfacing with the IPFS RPC API,
/// users of `ipfs-webdav` need to implement the `PeerApi` trait for their implementation
/// of an API that interfaces with the IPFS PRC API.
pub struct BaseApi {
    ipfs: IpfsClient,
}

impl BaseApi {
    /// Creates default instance of `BaseApi`
    pub fn new() -> Box<BaseApi> {
        BaseApi::from_ipfs_client(IpfsClient::default())
    }

    /// Creates a new instance of `BaseApi` from a provided IPFS API Server URI
    pub fn from_str(uri: &str) -> Box<BaseApi> {
        BaseApi::from_ipfs_client(IpfsClient::from_str(uri).unwrap())
    }

    /// Creates a new instance of `BaseApi` from provided `IpfsClient`
    pub fn from_ipfs_client(ipfs: IpfsClient) -> Box<BaseApi> {
        Box::new(BaseApi { ipfs })
    }
}

impl Debug for BaseApi {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BasicApi")
    }
}

#[async_trait]
impl PeerApi for BaseApi {
    async fn cp(&self, path: &String, dest: &String) -> Result<(), Error> {
        let path = normalize_path(path);
        let dest = normalize_path(dest);
        let ipfs = self.ipfs.clone();
        spawn_blocking(move || {
            block_on(async {
                ipfs.files_cp(&path, &dest).await
            })
        })
            .await.unwrap()
    }

    async fn flush(&self, path: &String) -> Result<(), Error> {
        let path = normalize_path(path);
        let ipfs = self.ipfs.clone();
        spawn_blocking(move || {
            block_on(async {
                ipfs.files_flush(Some(&path)).await
            })
        })
            .await.unwrap()
    }

    async fn ls(&self, path: &String) -> Result<Vec<PeerEntry>, Error> {
        let path = normalize_path(path);
        let ipfs = self.ipfs.clone();
        spawn_blocking(move || {
            block_on(async {
                let req = FilesLs {
                    path: Some(&path),
                    long: Some(true),
                    ..Default::default()
                };
                let res = ipfs.files_ls_with_options(req).await?;
                Ok(res.entries
                    .iter()
                    .map(|e| {
                        let p = concat_path(&path, &e.name);
                        PeerEntry::from_entry(&p, e)
                    })
                    .collect())
            })
        })
            .await.unwrap()
    }

    async fn mkdir(&self, path: &String) -> Result<PeerEntry, Error> {
        let path = normalize_path(path);
        let ipfs = self.ipfs.clone();
        spawn_blocking(move || {
            block_on(async {
                ipfs.files_mkdir(&path, false).await?;
                Ok(PeerEntry::new_dir(&path))
            })
        })
            .await.unwrap()
    }

    async fn mv(&self, path: &String, dest: &String) -> Result<(), Error> {
        let path = normalize_path(path);
        let dest = normalize_path(dest);
        let ipfs = self.ipfs.clone();
        spawn_blocking(move || {
            block_on(async {
                ipfs.files_mv(&path, &dest).await
            })
        })
            .await.unwrap()
    }

    async fn read(&self, path: &String, offset: usize, count: usize) -> Result<Bytes, Error> {
        let path = normalize_path(path);
        let ipfs = self.ipfs.clone();
        spawn_blocking(move || {
            block_on(async {
                let req = FilesRead {
                    path: &path,
                    offset: Some(offset as i64),
                    count: Some(count as i64),
                };
                let data = ipfs.files_read_with_options(req)
                    .map_ok(|chunk| chunk.to_vec())
                    .try_concat()
                    .await?;
                Ok(Bytes::copy_from_slice(&data))
            })
        })
            .await.unwrap()
    }

    async fn rm(&self, path: &String) -> Result<(), Error> {
        let path = normalize_path(path);
        let ipfs = self.ipfs.clone();
        spawn_blocking(move || {
            block_on(async {
                ipfs.files_rm(&path, true).await
            })
        })
            .await.unwrap()
    }

    async fn stat(&self, path: &String) -> Result<PeerEntry, Error> {
        let path = normalize_path(path);
        let ipfs = self.ipfs.clone();
        spawn_blocking(move || {
            block_on(async {
                let stat = ipfs.files_stat(&path).await?;
                Ok(PeerEntry::from_stat(&path, &stat))
            })
        })
            .await.unwrap()
    }

    async fn write(
        &self, path: &String, offset: usize, truncate: bool, data: Bytes,
    ) -> Result<(), Error> {
        let path = normalize_path(path);
        let ipfs = self.ipfs.clone();
        spawn_blocking(move || {
            block_on(async {
                let req = FilesWrite {
                    path: &path,
                    offset: Some(offset as i64),
                    create: Some(true),
                    truncate: Some(truncate),
                    flush: Some(false),
                    ..Default::default()
                };
                ipfs.files_write_with_options(req, data.reader()).await
            })
        })
            .await.unwrap()
    }
}

#[inline]
fn concat_path(p1: &String, p2: &String) -> String {
    pb_to_string(Path::new(p1).join(Path::new(p2)))
}

#[inline]
fn pb_to_string(path: PathBuf) -> String {
    path.into_os_string().into_string().unwrap()
}

#[inline]
fn normalize_path(path: &String) -> String {
    let mut path = path.clone();
    if path.len() > 1 && path.ends_with("/") {
        path.pop();
    }
    path
}
