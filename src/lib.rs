//
// Copyright (C) 2019 Kubos Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#![allow(clippy::block_in_if_condition_stmt)]

use log::debug;
use file_protocol::{FileProtocol, FileProtocolConfig, ProtocolError, State};
use kubos_system::Config as ServiceConfig;
use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::net::UdpSocket;
use notify::{Watcher, RecursiveMode, PollWatcher, EventKind};

// We need this in this lib.rs file so we can build integration tests
pub fn recv_loop(config: &ServiceConfig) -> Result<(), failure::Error> {
    // Get and bind our UDP listening socket
    let host = config
        .hosturl()
        .ok_or_else(|| failure::format_err!("Unable to fetch addr for service"))?;

    // Extract our local IP address so we can spawn child sockets later
    let mut host_parts = host.split(':').map(|val| val.to_owned());
    let host_ip = host_parts
        .next()
        .ok_or_else(|| failure::format_err!("Failed to parse service IP address"))?;

    // Get the storage directory prefix that we'll be using for our
    // temporary/intermediate storage location
    let prefix = match config.get("storage_dir") {
        Some(val) => val.as_str().map(|str| str.to_owned()),
        None => None,
    };

    // Get the chunk size to be used for transfers
    let transfer_chunk_size = match config.get("transfer_chunk_size") {
        Some(val) => val.as_integer().unwrap_or(896),
        None => 896,
    };

    // Get the chunk size to be used for hashing
    let hash_chunk_size = match config.get("hash_chunk_size") {
        Some(val) => val.as_integer().unwrap_or(transfer_chunk_size * 2),
        None => transfer_chunk_size * 2,
    } as usize;

    let transfer_chunk_size = transfer_chunk_size as usize;

    let hold_count = match config.get("hold_count") {
        Some(val) => val.as_integer().unwrap_or(5),
        None => 5,
    } as u16;

    // Get the downlink port we'll be using when sending responses
    let downlink_port = config
        .get("downlink_port")
        .and_then(|i| i.as_integer())
        .unwrap_or(8080) as u16;

    // Get the downlink ip we'll be using when sending responses
    let downlink_ip = match config.get("downlink_ip") {
        Some(ip) => match ip.as_str().map(|ip| ip.to_owned()) {
            Some(ip) => ip,
            None => "127.0.0.1".to_owned(),
        },
        None => "127.0.0.1".to_owned(),
    };

    // Get the inter chunk delay value
    let inter_chunk_delay = config
        .get("inter_chunk_delay")
        .and_then(|i| i.as_integer())
        .unwrap_or(1) as u64;

    // Get the max chunk transmission value
    let max_chunks_transmit = config
        .get("max_chunks_transmit")
        .and_then(|chunks| chunks.as_integer())
        .map(|chunks| chunks as u32);

    let num_threads = config
        .get("num_threads")
        .and_then(|i| i.as_integer())
        .unwrap_or(5) as u8;

    info!("Starting file transfer service");
    info!("Listening on {}", host);
    info!("Downlinking to {}:{}", downlink_ip, downlink_port);
    info!("Transfer Chunk {}", transfer_chunk_size);
    info!("Hash Chunk Size {}", hash_chunk_size);

    let f_config = FileProtocolConfig::new(
        prefix.clone(),
        transfer_chunk_size.clone(),
        hold_count,
        inter_chunk_delay,
        max_chunks_transmit,
        hash_chunk_size.clone(),
    );

    // Listen on UDP port
    let host_socket = UdpSocket::bind(host.clone())?;

    let timeout = config
        .get("timeout")
        .and_then(|val| val.as_integer().map(|num| Duration::from_secs(num as u64)))
        .unwrap_or(Duration::from_secs(2));

    // Setup map of channel IDs to thread channels
    let raw_threads: HashMap<u32, Sender<Vec<u8>>> = HashMap::new();
    // Create thread sharable wrapper
    let threads = Arc::new(Mutex::new(raw_threads));

    let stored_files = Arc::new(Mutex::new(HashMap::new()));
    let stored_files_clone = Arc::clone(&stored_files);

    let protocol_loop = thread::spawn(move || {


        loop {
            let mut buf = vec![0; hash_chunk_size];
            let (_source, first_message) = match host_socket.recv_from(&mut buf) {
                Ok((size, source)) => {
                    buf.truncate(size);
                    (source, buf)
                }
                Err(e) => {
                    warn!("Error receiving message: {:?}", e);
                    continue;
                }
            };
            debug!("Received message from {}", _source);
    
            let config_ref = f_config.clone();
            let host_ref = host_ip.clone();
            let timeout_ref = timeout;
    
            let channel_id = match file_protocol::parse_channel_id(&first_message) {
                Ok(channel_id) => channel_id,
                Err(e) => {
                    warn!("Error parsing channel ID: {:?}", e);
                    continue;
                }
            };
    
            if !threads
                .lock()
                .map_err(|err| {
                    error!("Failed to get threads mutex: {:?}", err);
                    err
                })
                .unwrap()
                .contains_key(&channel_id)
            {
                let (sender, receiver): (Sender<Vec<u8>>, Receiver<Vec<u8>>) =
                    mpsc::channel();
    
                threads
                    .lock()
                    .map_err(|err| {
                        error!("Failed to get threads mutex: {:?}", err);
                        err
                    })
                    .unwrap()
                    .insert(channel_id, sender.clone());
    
                // Break the processing work off into its own thread so we can
                // listen for requests from other clients
                let shared_threads = threads.clone();
                let downlink_ip_ref = downlink_ip.to_owned();
                let clone_stored_files = Arc::clone(&stored_files);
                thread::spawn(move || {
                    debug!("Starting new thread for channel {}", channel_id);
                    let state = State::Holding {
                        count: 0,
                        prev_state: Box::new(State::Done),
                    };
                    
                    // Set up the file system processor with the reply socket information
                    let f_protocol = FileProtocol::new(
                        &format!("{}:{}", host_ref, 0),
                        &format!("{}:{}", downlink_ip_ref, downlink_port),
                        config_ref,
                        num_threads,
                        clone_stored_files,
                    );
    
                    // Listen, process, and react to the remaining messages in the
                    // requested operation
                    if let Err(e) = f_protocol.message_engine(
                        |d| match receiver.recv_timeout(d) {
                            Ok(v) => Ok(v),
                            Err(RecvTimeoutError::Timeout) => Err(ProtocolError::ReceiveTimeout),
                            Err(e) => Err(ProtocolError::ReceiveError {
                                err: format!("Error {:?}", e),
                            }),
                        },
                        timeout_ref,
                        &state,
                    ) {
                        warn!("Encountered errors while processing transaction: {}", e);
                    }
    
                    // Remove ourselves from threads list if we are finished
                    shared_threads
                        .lock()
                        .map_err(|err| {
                            error!("Failed to get threads mutex: {:?}", err);
                            err
                        })
                        .unwrap()
                        .remove(&channel_id);
                });
            }
    
            if let Some(sender) = threads
                .lock()
                .map_err(|err| {
                    error!("Failed to get threads mutex: {:?}", err);
                    err
                })
                .unwrap()
                .get(&channel_id)
            {
                if let Err(e) = sender.send(first_message) {
                    warn!("Error when sending to channel {}: {:?}", channel_id, e);
                }
            }
    
            if !threads
                .lock()
                .map_err(|err| {
                    error!("Failed to get threads mutex: {:?}", err);
                    err
                })
                .unwrap()
                .contains_key(&channel_id)
            {
                warn!("No sender found for {}", channel_id);
                threads
                    .lock()
                    .map_err(|err| {
                        error!("Failed to get threads mutex: {:?}", err);
                        err
                    })
                    .unwrap()
                    .remove(&channel_id);
            }
        }
    });
    
    let file_initialise_loop = thread::spawn(move || {
        // Create a channel to receive events
        let (tx, rx) = mpsc::channel(); 

        let watcher_config = notify::Config::default()
            .with_poll_interval(Duration::from_secs(10));

        // Create a watcher object, delivering debounced events via the channel
        let mut watcher = PollWatcher::new(tx, watcher_config).unwrap();

        // Add a path to be watched. All files and directories at that path and below will be monitored for changes.
        watcher.watch(std::path::Path::new("/home/kubos/download/"), RecursiveMode::Recursive).unwrap();

        loop {
            match rx.recv() {
                Ok(Ok(event)) => {
                    match event.kind {
                        EventKind::Create(_) => {
                            let path = event.paths[0].clone();
                            // A new file was created, initialize it
                            match file_protocol::storage::initialize_file(&prefix.as_ref().unwrap(), path.to_str().unwrap(), transfer_chunk_size, hash_chunk_size) {
                                Ok((file_name, hash, num_chunks, mode)) => {
                                    stored_files_clone.lock().unwrap().insert(
                                        path.to_str().unwrap().to_string(),
                                        (file_name, hash, num_chunks, mode),
                                    );
                                },
                                Err(e) => println!("Error initializing file: {:?}", e),
                            }
                        },
                        _ => {},
                    } 
                },
                Ok(Err(e)) => println!("watch error: {:?}", e),
                Err(e) => println!("watch error: {:?}", e),
            }
        }        
    });

    protocol_loop.join().unwrap();
    file_initialise_loop.join().unwrap();

    Ok(())
}
