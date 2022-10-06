// Copyright 2022 Debox Developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{Error, ErrorKind, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use bytes::{Buf, Bytes};
use futures::{future, stream};
use futures::future::{BoxFuture, FutureExt};
use http::StatusCode;
use webdav_handler::davpath::DavPath;
use webdav_handler::fs::{
    DavDirEntry, DavFile, DavFileSystem, DavMetaData, DavProp, FsError, FsFuture, FsResult,
    FsStream, OpenOptions, ReadDirMeta,
};

use crate::api::{PeerApi, PeerEntry};
use crate::cache::Cache;

#[derive(Debug, Clone)]
pub(crate) struct PeerFs {
    api: Arc<Box<dyn PeerApi>>,
    cache: Cache,
}

#[derive(Debug, Clone)]
pub(crate) enum PeerNode {
    Dir(PeerDirNode),
    File(PeerFileNode),
}

#[derive(Debug, Clone)]
pub(crate) struct PeerDirNode {
    mtime: SystemTime,
    crtime: SystemTime,
    props: HashMap<String, DavProp>,
}

#[derive(Debug, Clone)]
pub(crate) struct PeerFileNode {
    mtime: SystemTime,
    crtime: SystemTime,
    props: HashMap<String, DavProp>,
    size: usize,
}

#[derive(Debug, Clone)]
struct PeerFsEntry {
    mtime: SystemTime,
    crtime: SystemTime,
    is_dir: bool,
    name: Vec<u8>,
    size: usize,
}

#[derive(Debug)]
struct PeerFsFile {
    api: Arc<Box<dyn PeerApi>>,
    cache: Cache,
    path: String,
    mtime: SystemTime,
    crtime: SystemTime,
    pos: usize,
    size: usize,
    append: bool,
    truncate: bool,
}

impl PeerFs {
    pub(crate) fn new(api: Box<dyn PeerApi>) -> Box<PeerFs> {
        Box::new(PeerFs {
            api: Arc::new(api),
            cache: Cache::default(),
        })
    }

    fn do_open(&self, path: &String, options: OpenOptions) -> FsResult<Box<dyn DavFile>> {
        let node = match self.cache.get(&path) {
            Ok(node) => {
                if options.create_new {
                    return Err(FsError::Exists);
                } else if node.is_dir() {
                    return Err(FsError::Forbidden);
                }
                Some(node)
            }
            Err(FsError::NotFound) => {
                if !options.create {
                    return Err(FsError::NotFound);
                }
                None
            }
            Err(e) => return Err(e),
        };

        let size = match node {
            None => 0,
            Some(node) => node.as_file().unwrap().size,
        };

        Ok(Box::new(PeerFsFile {
            api: self.api.clone(),
            cache: self.cache.clone(),
            path: path.clone(),
            crtime: SystemTime::now(),
            mtime: SystemTime::now(),
            pos: 0,
            size,
            append: options.append,
            truncate: options.truncate,
        }))
    }
}

impl DavFileSystem for PeerFs {
    fn open<'a>(&'a self, path: &'a DavPath, options: OpenOptions) -> FsFuture<Box<dyn DavFile>> {
        async move {
            trace!("DFS: open {:?}", path);
            let path = path_to_string(path);
            self.do_open(&path, options)
        }
            .boxed()
    }

    fn read_dir<'a>(
        &'a self, path: &'a DavPath, _meta: ReadDirMeta,
    ) -> FsFuture<FsStream<Box<dyn DavDirEntry>>>
    {
        async move {
            trace!("DFS: read_dir {:?}", path);
            let path = path_to_string(path);
            let mut v: Vec<Box<dyn DavDirEntry>> = Vec::new();
            if let Ok(entries) = self.api.ls(&path).await {
                for entry in entries {
                    let node = PeerNode::from_api_entry(&entry);
                    v.push(Box::new(node.to_entry(&entry.path)));
                    self.cache.insert(&entry.path, node);
                }
            }
            let stream = stream::iter(v.into_iter());
            Ok(Box::pin(stream) as FsStream<Box<dyn DavDirEntry>>)
        }
            .boxed()
    }

    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<Box<dyn DavMetaData>> {
        async move {
            let path = path_to_string(path);
            if !self.cache.contains(&path) {
                if let Ok(entry) = self.api.stat(&path).await {
                    self.cache.insert(&path, PeerNode::from_api_entry(&entry));
                }
            }
            let entry = self.cache.get(&path)?.to_entry(&path);
            Ok(Box::new(entry) as Box<dyn DavMetaData>)
        }
            .boxed()
    }

    fn create_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        async move {
            trace!("DFS: create_dir {:?}", path);
            let path = path_to_string(path);
            if let Ok(_) = self.cache.get(&path) {
                return Err(FsError::Exists);
            }
            let parent = parent_path(&path);
            if parent != "/" && !self.cache.get(&parent)?.is_dir() {
                return Err(FsError::Forbidden);
            }
            if let Ok(entry) = self.api.mkdir(&path).await {
                self.cache.insert(&path, PeerNode::from_api_entry(&entry));
            }
            Ok(())
        }
            .boxed()
    }

    fn remove_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        async move {
            trace!("DFS: remove_dir {:?}", path);
            let path = path_to_string(path);
            if let Ok(_) = self.api.rm(&path).await {
                self.cache.remove(&path);
            }
            Ok(())
        }
            .boxed()
    }

    fn remove_file<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        async move {
            trace!("DFS: remove_file {:?}", path);
            let path = path_to_string(path);
            if let Ok(_) = self.api.rm(&path).await {
                self.cache.remove(&path);
            }
            Ok(())
        }
            .boxed()
    }

    fn rename<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<()> {
        async move {
            trace!("DFS: rename {:?} {:?}", from, to);
            let from = path_to_string(from);
            let to = path_to_string(to);
            if let Ok(_) = self.api.mv(&from, &to).await {
                self.cache.mv_vals(&from, &to);
            }
            Ok(())
        }
            .boxed()
    }

    fn copy<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<()> {
        async move {
            trace!("DFS: copy {:?} {:?}", from, to);
            let from = path_to_string(from);
            let to = path_to_string(to);
            if let Ok(_) = self.api.cp(&from, &to).await {
                self.cache.cp_vals(&from, &to);
            }
            Ok(())
        }
            .boxed()
    }

    fn have_props<'a>(&'a self, _path: &'a DavPath) -> BoxFuture<'a, bool> {
        future::ready(true).boxed()
    }

    fn patch_props<'a>(
        &'a self, path: &'a DavPath, mut patch: Vec<(bool, DavProp)>,
    ) -> FsFuture<Vec<(StatusCode, DavProp)>>
    {
        async move {
            let path = path_to_string(path);
            let node = &mut self.cache.get(&path)?;
            let props = node.props_mut();

            let mut res = Vec::new();
            let patch = patch.drain(..).collect::<Vec<_>>();
            for (set, p) in patch.into_iter() {
                let prop = clone_prop(&p);
                let status = if set {
                    props.insert(prop_key(&p.namespace, &p.name), p);
                    StatusCode::OK
                } else {
                    props.remove(&prop_key(&p.namespace, &p.name));
                    // the below map was added to signify if the remove succeeded or
                    // failed. however it seems that removing non-existent properties
                    // always succeed, so just return success.
                    //  .map(|_| StatusCode::OK).unwrap_or(StatusCode::NOT_FOUND)
                    StatusCode::OK
                };
                res.push((status, prop));
            }
            self.cache.insert(&path, node.to_owned());
            Ok(res)
        }
            .boxed()
    }

    fn get_props<'a>(&'a self, path: &'a DavPath, do_content: bool) -> FsFuture<Vec<DavProp>> {
        async move {
            let path = path_to_string(path);
            let node = &self.cache.get(&path)?;
            let mut res = Vec::new();
            for (_, p) in node.props() {
                res.push(if do_content { p.clone() } else { clone_prop(p) });
            }
            Ok(res)
        }
            .boxed()
    }

    fn get_prop<'a>(&'a self, path: &'a DavPath, prop: DavProp) -> FsFuture<Vec<u8>> {
        async move {
            let path = path_to_string(path);
            let node = &self.cache.get(&path)?;
            let p = node
                .props()
                .get(&prop_key(&prop.namespace, &prop.name))
                .ok_or(FsError::NotFound)?;
            Ok(p.xml.clone().ok_or(FsError::NotFound)?)
        }
            .boxed()
    }
}

impl PeerNode {
    fn from_api_entry(entry: &PeerEntry) -> Self {
        if entry.is_dir {
            PeerNode::Dir(PeerDirNode {
                crtime: entry.crtime,
                mtime: entry.mtime,
                props: HashMap::new(),
            })
        } else {
            PeerNode::File(PeerFileNode {
                crtime: entry.crtime,
                mtime: entry.mtime,
                props: HashMap::new(),
                size: entry.size,
            })
        }
    }

    fn from_fs_file(file: &PeerFsFile) -> Self {
        PeerNode::File(PeerFileNode {
            crtime: file.crtime,
            mtime: file.mtime,
            props: HashMap::new(),
            size: file.size,
        })
    }

    // Helper to create PeerFsDirEntry from a node
    fn to_entry(&self, path: &String) -> PeerFsEntry {
        let name = match Path::new(&path).file_name() {
            None => "/",
            Some(s) => s.to_str().unwrap(),
        }
            .as_bytes()
            .to_vec();
        let (is_dir, size, mtime, crtime) = match self {
            &PeerNode::Dir(ref d) => (true, 0, d.mtime, d.crtime),
            &PeerNode::File(ref f) => (false, f.size, f.mtime, f.crtime),
        };
        PeerFsEntry { mtime, crtime, is_dir, name, size }
    }

    fn is_dir(&self) -> bool {
        match self {
            &PeerNode::Dir(_) => true,
            &PeerNode::File(_) => false,
        }
    }

    fn as_file(&self) -> FsResult<&PeerFileNode> {
        match self {
            &PeerNode::File(ref n) => Ok(n),
            _ => Err(FsError::Forbidden),
        }
    }

    fn props(&self) -> &HashMap<String, DavProp> {
        match self {
            &PeerNode::Dir(ref d) => &d.props,
            &PeerNode::File(ref f) => &f.props,
        }
    }

    fn props_mut(&mut self) -> &mut HashMap<String, DavProp> {
        match self {
            &mut PeerNode::Dir(ref mut d) => &mut d.props,
            &mut PeerNode::File(ref mut f) => &mut f.props,
        }
    }
}

impl DavDirEntry for PeerFsEntry {
    fn name(&self) -> Vec<u8> {
        self.name.clone()
    }

    fn metadata<'a>(&'a self) -> FsFuture<Box<dyn DavMetaData>> {
        let meta = (*self).clone();
        Box::pin(future::ok(Box::new(meta) as Box<dyn DavMetaData>))
    }
}

impl DavMetaData for PeerFsEntry {
    fn len(&self) -> u64 {
        self.size as u64
    }

    fn modified(&self) -> FsResult<SystemTime> {
        Ok(self.mtime)
    }

    fn is_dir(&self) -> bool {
        self.is_dir
    }

    fn created(&self) -> FsResult<SystemTime> {
        Ok(self.crtime)
    }
}

impl PeerFsFile {
    async fn do_write(&mut self, buf: Bytes) {
        if self.append {
            self.pos = self.size;
        }
        self.size = self.pos + buf.len();
        let _ = self.api.write(&self.path, self.pos, self.truncate, buf).await;
        self.pos = self.size;
        self.truncate = false;
    }
}

impl DavFile for PeerFsFile {
    fn metadata<'a>(&'a mut self) -> FsFuture<Box<dyn DavMetaData>> {
        async move {
            let entry = self.cache.get(&self.path)?.to_entry(&self.path);
            Ok(Box::new(entry) as Box<dyn DavMetaData>)
        }
            .boxed()
    }

    fn write_buf<'a>(&'a mut self, mut buf: Box<dyn Buf + Send>) -> FsFuture<()> {
        async move {
            trace!("DF: write_buf");
            while buf.has_remaining() {
                let b = buf.chunk();
                let len = b.len();
                self.do_write(Bytes::copy_from_slice(b)).await;
                buf.advance(len);
            }
            Ok(())
        }
            .boxed()
    }

    fn write_bytes(&mut self, buf: Bytes) -> FsFuture<()> {
        async move {
            trace!("DF: write_bytes");
            self.do_write(buf).await;
            Ok(())
        }
            .boxed()
    }

    fn read_bytes(&mut self, count: usize) -> FsFuture<Bytes> {
        async move {
            trace!("DF: read_bytes ({:?} bytes)", count);
            let res = self.api.read(&self.path, self.pos, count).await;
            self.pos += count;
            match res {
                Ok(bytes) => Ok(bytes),
                Err(_) => Err(FsError::GeneralFailure),
            }
        }
            .boxed()
    }

    fn seek(&mut self, pos: SeekFrom) -> FsFuture<u64> {
        async move {
            trace!("DF: seek");
            let (start, offset): (u64, i64) = match pos {
                SeekFrom::Start(pos) => {
                    self.pos = pos as usize;
                    return Ok(pos);
                }
                SeekFrom::Current(pos) => (self.pos as u64, pos),
                SeekFrom::End(pos) => (self.size as u64, pos),
            };
            if offset < 0 {
                if -offset as u64 > start {
                    return Err(Error::new(ErrorKind::InvalidInput, "invalid seek").into());
                }
                self.pos = (start - (-offset as u64)) as usize;
            } else {
                self.pos = (start + offset as u64) as usize;
            }
            Ok(self.pos as u64)
        }
            .boxed()
    }

    fn flush(&mut self) -> FsFuture<()> {
        async move {
            trace!("DF: flush");
            if let Ok(..) = self.api.flush(&self.path).await {
                self.cache.insert(&self.path, PeerNode::from_fs_file(&self));
            }
            Ok(())
        }
            .boxed()
    }
}

#[inline]
fn path_to_string(path: &DavPath) -> String {
    pb_to_string(path.as_pathbuf())
}

#[inline]
fn parent_path(path: &String) -> String {
    pb_to_string(Path::new(path).parent().unwrap().to_path_buf())
}

#[inline]
fn pb_to_string(path: PathBuf) -> String {
    path.into_os_string().into_string().unwrap()
}

#[inline]
fn prop_key(ns: &Option<String>, name: &str) -> String {
    ns.to_owned().as_ref().unwrap_or(&"".to_string()).clone() + name
}

#[inline]
fn clone_prop(p: &DavProp) -> DavProp {
    DavProp {
        name: p.name.clone(),
        namespace: p.namespace.clone(),
        prefix: p.prefix.clone(),
        xml: None,
    }
}
