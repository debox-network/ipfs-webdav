// Copyright 2022 Debox Developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use webdav_handler::fs::FsError;

use crate::fs::PeerNode;

#[derive(Default, Debug, Clone)]
pub(crate) struct Cache {
    cache: Arc<Mutex<HashMap<String, PeerNode>>>,
}

impl Cache {
    pub(crate) fn contains(&self, hash: &String) -> bool {
        let hash = normalize_hash(hash);
        let cache = &*self.cache.lock().unwrap();
        cache.contains_key(&hash)
    }

    pub(crate) fn get(&self, hash: &String) -> Result<PeerNode, FsError> {
        let hash = normalize_hash(hash);
        let cache = &*self.cache.lock().unwrap();
        match cache.get(&hash) {
            None => Err(FsError::NotFound),
            Some(value) => Ok(value.clone()),
        }
    }

    pub(crate) fn insert(&self, hash: &String, node: PeerNode) {
        let hash = normalize_hash(hash);
        let cache = &mut *self.cache.lock().unwrap();
        cache.insert(hash, node);
    }

    pub(crate) fn remove(&self, hash: &String) {
        let hash = normalize_hash(hash);
        let cache = &mut *self.cache.lock().unwrap();
        cache.remove(&hash);
    }

    pub(crate) fn mv_vals(&self, from: &String, to: &String) {
        let from = normalize_hash(from);
        let to = normalize_hash(to);
        let prefix = add_slash(&from);
        let cache = &mut *self.cache.lock().unwrap();
        cache.clone()
            .iter()
            .filter(|&(k, _)| {
                k == &from || k.starts_with(&prefix)
            })
            .for_each(|(k, _)| {
                if let Some(v) = cache.remove(k) {
                    let k = k.replace(from.as_str(), to.as_str());
                    cache.insert(k, v);
                }
            });
    }

    pub(crate) fn cp_vals(&self, from: &String, to: &String) {
        let from = normalize_hash(from);
        let to = normalize_hash(to);
        let prefix = add_slash(&from);
        let cache = &mut *self.cache.lock().unwrap();
        cache.clone()
            .iter()
            .filter(|&(k, _)| k == &from || k.starts_with(&prefix))
            .for_each(|(k, _)| {
                if let Some(v) = cache.get(k) {
                    let k = k.replace(from.as_str(), to.as_str());
                    cache.insert(k, v.clone());
                }
            });
    }
}

#[inline]
fn normalize_hash(hash: &String) -> String {
    let mut hash = hash.clone();
    if hash.ends_with("/") {
        hash.pop();
    }
    hash
}

#[inline]
fn add_slash(hash: &String) -> String {
    let mut hash = hash.clone();
    if !hash.ends_with("/") {
        hash.push('/');
    }
    hash
}
