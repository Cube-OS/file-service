//
// Copyright (C) 2018 Kubos Corporation
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
#![macro_use]
#![allow(dead_code)]

use std::collections::HashMap;
use blake2_rfc::blake2s::Blake2s;
use file_protocol::{FileProtocol, FileProtocolConfig, Message, ProtocolError, State};
use std::fs::File;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[macro_export]
macro_rules! service_new {
    ($port:expr, $down_port:expr, $chunk_size:expr, $storage_dir:expr) => {{
        thread::spawn(move || {
            recv_loop(
                &ServiceConfig::new_from_str(
                    "file-transfer-service",
                    &format!(
                        r#"
                [file-transfer-service]
                storage_dir = "{}"
                transfer_chunk_size = {}
                hold_count = 5
                downlink_ip = "127.0.0.1"
                downlink_port = {}
                [file-transfer-service.addr]
                ip = "127.0.0.1"
                port = {}
                "#,
                        $storage_dir, $chunk_size, $down_port, $port
                    ),
                )
                .unwrap(),
            )
            .unwrap();
        });

        thread::sleep(Duration::new(1, 0));
    }};
}

pub fn download(
    host_ip: &str,
    host_port: u16,
    remote_addr: &str,
    source_path: &str,
    target_path: &str,
    prefix: Option<String>,
    chunk_size: u32,
) -> Result<(), ProtocolError> {
    let hold_count = 5;
    let f_config = FileProtocolConfig::new(
        prefix,
        chunk_size as usize,
        hold_count,
        1,
        None,
        (chunk_size as usize) * 2,
    );
    let f_protocol =
        FileProtocol::new(&format!("{}:{}", host_ip, host_port), remote_addr, f_config, 1, Arc::new(Mutex::new(HashMap::new())),);

    let channel = f_protocol.generate_channel()?;

    // Send our file request to the remote addr and verify that it's
    // going to be able to send it
    f_protocol.send_import_file(channel, source_path)?;

    // Wait for the request reply.
    // Note/TODO: We don't use a timeout here because we don't know how long it will
    // take the server to prepare the file we've requested.
    // Larger files (> 100MB) can take over a minute to process.
    let reply = match f_protocol.recv(None) {
        Ok(message) => message,
        Err(error) => return Err(error),
    };

    let state = f_protocol.process_message(
        reply.as_slice(),
        &State::StartReceive {
            path: target_path.to_string(),
        },
    )?;

    Ok(f_protocol.message_engine(|d| f_protocol.recv(Some(d)), Duration::from_secs(2), &state)?)
}

pub fn download_partial(
    host_ip: &str,
    host_port: u16,
    remote_addr: &str,
    source_path: &str,
    target_path: &str,
    prefix: Option<String>,
    chunk_size: u32,
) -> Result<(), ProtocolError> {
    let hold_count = 5;
    let f_config = FileProtocolConfig::new(
        prefix,
        chunk_size as usize,
        hold_count,
        1,
        None,
        (chunk_size as usize) * 2,
    );
    let f_protocol =
        FileProtocol::new(&format!("{}:{}", host_ip, host_port), remote_addr, f_config, 1, Arc::new(Mutex::new(HashMap::new())),);

    let channel = f_protocol.generate_channel()?;

    // Send our file request to the remote addr and verify that it's
    // going to be able to send it
    f_protocol.send_import_file(channel, source_path)?;

    // Wait for the request reply.
    // Note/TODO: We don't use a timeout here because we don't know how long it will
    // take the server to prepare the file we've requested.
    // Larger files (> 100MB) can take over a minute to process.
    let reply = match f_protocol.recv(None) {
        Ok(message) => message,
        Err(error) => return Err(error),
    };

    // Modify the reply so that we don't attempt to download
    // all of the chunks
    let reply = bincode::deserialize::<Message>(&reply)?;

    // Recreate the reply but ask for one less chunk so we don't download
    // the whole file this time
    let new_reply = match reply {
        Message::ReceiveChunk { channel_id, hash, chunk_num, data } => {
            Message::ReceiveChunk {
                channel_id,
                hash,
                chunk_num: chunk_num - 1,
                data,
            }
        },
        Message::SuccessTransmit { channel_id, file_name, hash, num_chunks, mode, last } => {
            Message::SuccessTransmit {
                channel_id,
                file_name,
                hash,
                num_chunks: num_chunks - 1,
                mode,
                last,
            }
        },
        _ => panic!("Unexpected message received: {:?}", reply),
    };

    let state = f_protocol.process_message(
        &bincode::serialize::<Message>(&new_reply)?,
        &State::StartReceive {
            path: target_path.to_string(),
        },
    )?;

    Ok(f_protocol.message_engine(|d| f_protocol.recv(Some(d)), Duration::from_secs(2), &state)?)
}

pub fn upload(
    host_ip: &str,
    host_port: u16,
    remote_addr: &str,
    source_path: &str,
    target_path: &str,
    prefix: Option<String>,
    chunk_size: u32,
) -> Result<String, ProtocolError> {
    let hold_count = 5;
    let f_config = FileProtocolConfig::new(
        prefix,
        chunk_size as usize,
        hold_count,
        1,
        None,
        (chunk_size as usize) * 2,
    );
    let f_protocol =
        FileProtocol::new(&format!("{}:{}", host_ip, host_port), remote_addr, f_config, 1, Arc::new(Mutex::new(HashMap::new())),);

    // copy file to upload to temp storage. calculate the hash and chunk info
    let (_filename, hash, num_chunks, mode) = f_protocol.initialize_file(&source_path)?;

    let channel = f_protocol.generate_channel()?;

    // tell our destination the hash and number of chunks to expect
    f_protocol.send_metadata(channel, &hash, num_chunks)?;

    // send export command for file
    f_protocol.send_export(channel, &hash, &target_path, mode)?;

    // start the engine to send the file data chunks
    f_protocol.message_engine(
        |d| f_protocol.recv(Some(d)),
        Duration::from_secs(2),
        &State::Transmitting { transmitted_files: 0, total_files: 1 },
    )?;

    // note: the original upload client function does not return the hash.
    // we're only doing it here so that we can manipulate the temporary storage
    Ok(hash.to_owned())
}

pub fn upload_partial(
    host_ip: &str,
    host_port: u16,
    remote_addr: &str,
    source_path: &str,
    target_path: &str,
    prefix: Option<String>,
    chunk_size: u32,
) -> Result<String, ProtocolError> {
    let hold_count = 5;
    let f_config = FileProtocolConfig::new(
        prefix,
        chunk_size as usize,
        hold_count,
        1,
        None,
        (chunk_size as usize) * 2,
    );
    let f_protocol =
        FileProtocol::new(&format!("{}:{}", host_ip, host_port), remote_addr, f_config, 1, Arc::new(Mutex::new(HashMap::new())),);

    // Copy file to upload to temp storage. calculate the hash and chunk info
    let (_filename, hash, num_chunks, mode) = f_protocol.initialize_file(&source_path)?;

    let channel = f_protocol.generate_channel()?;

    // Tell our destination the hash and number of chunks (- 1) to expect
    f_protocol.send_metadata(channel, &hash, num_chunks - 1)?;

    // Send export command for file
    f_protocol.send_export(channel, &hash, &target_path, mode)?;

    // Start the engine to send the file data chunks
    f_protocol.message_engine(
        |d| f_protocol.recv(Some(d)),
        Duration::from_secs(2),
        &State::Transmitting { transmitted_files: 0, total_files: 1 },
    )?;

    // Note: The original upload client function does not return the hash.
    // we're only doing it here so that we can manipulate the temporary storage
    Ok(hash.to_owned())
}

pub fn cleanup(
    host_ip: &str,
    host_port: u16,
    remote_addr: &str,
    hash: Option<String>,
    prefix: Option<String>,
    chunk_size: u32,
) -> Result<(), ProtocolError> {
    let hold_count = 5;
    let f_config = FileProtocolConfig::new(
        prefix,
        chunk_size as usize,
        hold_count,
        1,
        None,
        (chunk_size as usize) * 2,
    );
    let f_protocol =
        FileProtocol::new(&format!("{}:{}", host_ip, host_port), remote_addr, f_config, 1, Arc::new(Mutex::new(HashMap::new())),);

    let channel = f_protocol.generate_channel()?;

    // Request the remote side to perform a cleanup
    f_protocol.send_cleanup(channel, hash)?;

    thread::sleep(Duration::from_millis(100));

    Ok(())
}

pub fn create_test_file(name: &str, contents: &[u8]) -> String {
    let mut file = File::create(name).unwrap();
    file.write_all(contents).unwrap();

    let mut hasher = Blake2s::new(16);
    hasher.update(contents);
    let hash = hasher.finalize();

    let hash_str = hash
        .as_bytes()
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect();

    hash_str
}
