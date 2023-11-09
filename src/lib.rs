// Copyright 2022-2023 Debox Network
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
//

#[macro_use]
extern crate log;

use webdav_handler::memls::MemLs;
use webdav_handler::DavHandler;

use crate::api::PeerApi;
use crate::fs::PeerFs;

pub mod api;

mod cache;
mod fs;

/// Creates a WebDAV handler
pub fn make_server(api: Box<dyn PeerApi>) -> DavHandler {
    DavHandler::builder()
        .filesystem(PeerFs::new(api))
        .locksystem(MemLs::new())
        .build_handler()
}
