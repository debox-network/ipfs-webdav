// Copyright 2022-2023 Debox Network
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use webdav_handler::fs::FsError;

use crate::fs::PeerNode;

#[derive(Default, Debug, Clone)]
pub(super) struct Cache {
    cache: Arc<RwLock<HashMap<String, PeerNode>>>,
}

impl Cache {
    pub(super) fn contains(&self, hash: &str) -> bool {
        let hash = normalize_hash(hash);
        let cache = self.cache.read().unwrap();
        cache.contains_key(&hash)
    }

    pub(super) fn get(&self, hash: &str) -> Result<PeerNode, FsError> {
        let hash = normalize_hash(hash);
        let cache = self.cache.read().unwrap();
        match cache.get(&hash) {
            None => Err(FsError::NotFound),
            Some(value) => Ok(value.clone()),
        }
    }

    pub(super) fn insert(&self, hash: &str, node: PeerNode) {
        let hash = normalize_hash(hash);
        let cache = &mut self.cache.write().unwrap();
        cache.insert(hash, node);
    }

    pub(super) fn remove(&self, hash: &str) {
        let hash = normalize_hash(hash);
        let cache = &mut self.cache.write().unwrap();
        cache.remove(&hash);
    }

    pub(super) fn mv_vals(&self, from: &str, to: &str) {
        let from = normalize_hash(from);
        let to = normalize_hash(to);
        let prefix = add_slash(&from);
        let cache = &mut *self.cache.write().unwrap();
        cache
            .clone()
            .iter()
            .filter(|&(k, _)| k == &from || k.starts_with(&prefix))
            .for_each(|(k, _)| {
                if let Some(v) = cache.remove(k) {
                    let k = k.replace(from.as_str(), to.as_str());
                    cache.insert(k, v);
                }
            });
    }

    pub(super) fn cp_vals(&self, from: &str, to: &str) {
        let from = normalize_hash(from);
        let to = normalize_hash(to);
        let prefix = add_slash(&from);
        let cache = &mut *self.cache.write().unwrap();
        cache
            .clone()
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
fn normalize_hash(hash: &str) -> String {
    let mut hash = hash.to_string();
    if hash.ends_with('/') {
        hash.pop();
    }
    hash
}

#[inline]
fn add_slash(hash: &str) -> String {
    let mut hash = hash.to_string();
    if !hash.ends_with('/') {
        hash.push('/');
    }
    hash
}
