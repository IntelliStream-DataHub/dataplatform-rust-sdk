// SPDX-License-Identifier: Apache-2.0
//! Shared test scaffolding: a tiny HTTP/1.1 mock of the api, and RAII wrappers over the C handles
//! so a test reads like the C it stands in for without leaking on an assertion failure.
#![allow(dead_code)]

use std::ffi::{c_char, CStr, CString};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use intellistream_datahub::*;

// ---------------------------------------------------------------------------------------------
// Mock api
// ---------------------------------------------------------------------------------------------

/// One request the mock received.
#[derive(Clone, Debug)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.body)
            .unwrap_or_else(|e| panic!("body is not JSON ({e}): {}", self.body))
    }
}

type Handler = dyn Fn(&Request) -> (u16, String) + Send + Sync;

/// A single-threaded HTTP/1.1 server answering from a handler, recording every request. It
/// closes each connection after answering, which reqwest handles without complaint.
pub struct MockServer {
    pub base_url: String,
    requests: Arc<Mutex<Vec<Request>>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl MockServer {
    pub fn start(handler: impl Fn(&Request) -> (u16, String) + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let port = listener.local_addr().unwrap().port();
        let requests: Arc<Mutex<Vec<Request>>> = Arc::default();
        let stop = Arc::new(AtomicBool::new(false));
        let handler: Arc<Handler> = Arc::new(handler);
        let thread = {
            let (requests, stop) = (requests.clone(), stop.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => serve(stream, handler.as_ref(), &requests),
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        Err(_) => break,
                    }
                }
            })
        };
        MockServer {
            base_url: format!("http://127.0.0.1:{port}"),
            requests,
            stop,
            thread: Some(thread),
        }
    }

    pub fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }

    pub fn last_request(&self) -> Request {
        self.requests().pop().expect("the mock received no request")
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn serve(mut stream: TcpStream, handler: &Handler, requests: &Mutex<Vec<Request>>) {
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();
    if reader.read_line(&mut line).unwrap_or(0) == 0 {
        return;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();
    let mut headers = Vec::new();
    let mut content_length = 0usize;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).unwrap_or(0) == 0 || header.trim().is_empty() {
            break;
        }
        if let Some((name, value)) = header.trim_end().split_once(':') {
            let (name, value) = (name.trim().to_string(), value.trim().to_string());
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.parse().unwrap_or(0);
            }
            headers.push((name, value));
        }
    }
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body).unwrap();
    }
    let request = Request {
        method,
        path,
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    };
    let (status, response_body) = handler(&request);
    requests.lock().unwrap().push(request);
    let reason = match status {
        204 => "No Content",
        200 => "OK",
        _ => "Status",
    };
    let _ = write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        response_body.len(),
        response_body
    );
    let _ = stream.flush();
}

/// A handler answering every request the same way.
pub fn always(
    status: u16,
    body: &str,
) -> impl Fn(&Request) -> (u16, String) + Send + Sync + 'static {
    let body = body.to_string();
    move |_| (status, body.clone())
}

// ---------------------------------------------------------------------------------------------
// C-side helpers
// ---------------------------------------------------------------------------------------------

pub fn cstr(text: &str) -> CString {
    CString::new(text).unwrap()
}

/// The last error on this thread, as a Rust string.
pub fn last_error() -> String {
    unsafe { CStr::from_ptr(datahub_last_error()) }
        .to_string_lossy()
        .into_owned()
}

/// Take ownership of a string the library handed out and free it.
pub unsafe fn take_string(ptr: *mut c_char) -> String {
    assert!(!ptr.is_null(), "the library handed out a NULL string");
    let text = CStr::from_ptr(ptr).to_string_lossy().into_owned();
    datahub_string_free(ptr);
    text
}

pub unsafe fn borrow_string(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
    }
}

/// RAII over `datahub_config`.
pub struct Config(pub *mut datahub_config);

impl Config {
    pub fn new() -> Self {
        Config(datahub_config_new())
    }

    /// A config pointed at `base_url` with a static token, optionally spooling to `buffer_dir`.
    pub fn for_server(base_url: &str, buffer_dir: Option<&std::path::Path>) -> Self {
        let config = Config::new();
        config.set("BASE_URL", base_url);
        config.set("TOKEN", "test-token");
        if let Some(dir) = buffer_dir {
            let dir = cstr(dir.to_str().unwrap());
            assert_eq!(
                unsafe { datahub_config_set_buffer_dir(config.0, dir.as_ptr()) },
                datahub_status::DATAHUB_OK
            );
        }
        config
    }

    pub fn set(&self, key: &str, value: &str) {
        let (key, value) = (cstr(key), cstr(value));
        assert_eq!(
            unsafe { datahub_config_set(self.0, key.as_ptr(), value.as_ptr()) },
            datahub_status::DATAHUB_OK
        );
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let key = cstr(key);
        unsafe { borrow_string(datahub_config_get(self.0, key.as_ptr())) }
    }

    pub fn build(&self) -> Result<Client, datahub_status> {
        let mut out = std::ptr::null_mut();
        match unsafe { datahub_client_new(self.0, &mut out) } {
            datahub_status::DATAHUB_OK => {
                assert!(!out.is_null());
                Ok(Client(out))
            }
            status => {
                assert!(
                    out.is_null(),
                    "a failed datahub_client_new must leave *out NULL"
                );
                Err(status)
            }
        }
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        unsafe { datahub_config_free(self.0) }
    }
}

/// RAII over `datahub_client`.
#[derive(Debug)]
pub struct Client(pub *mut datahub_client);

impl Client {
    pub fn insert(&self, external_id: &str, points: &[datahub_datapoint]) -> datahub_status {
        let id = cstr(external_id);
        unsafe { datahub_datapoints_insert(self.0, id.as_ptr(), points.as_ptr(), points.len()) }
    }

    pub fn request_json(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> Result<String, datahub_status> {
        let (method, path) = (cstr(method), cstr(path));
        let body = body.map(cstr);
        let mut out = std::ptr::null_mut();
        let status = unsafe {
            datahub_request_json(
                self.0,
                method.as_ptr(),
                path.as_ptr(),
                body.as_ref().map_or(std::ptr::null(), |b| b.as_ptr()),
                &mut out,
            )
        };
        match status {
            datahub_status::DATAHUB_OK => Ok(unsafe { take_string(out) }),
            status => Err(status),
        }
    }

    /// Call one of the `..._json(client, body, out)` functions.
    pub fn json_call(
        &self,
        call: unsafe extern "C" fn(
            *const datahub_client,
            *const c_char,
            *mut *mut c_char,
        ) -> datahub_status,
        body: &str,
    ) -> (datahub_status, Option<String>) {
        let body = cstr(body);
        let mut out = std::ptr::null_mut();
        let status = unsafe { call(self.0, body.as_ptr(), &mut out) };
        let text = if out.is_null() {
            None
        } else {
            Some(unsafe { take_string(out) })
        };
        (status, text)
    }

    pub fn flush(&self) -> datahub_status {
        unsafe { datahub_client_flush(self.0) }
    }

    pub fn buffered_count(&self) -> u64 {
        unsafe { datahub_client_buffered_count(self.0) }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        unsafe { datahub_client_free(self.0) }
    }
}

pub fn point(timestamp_ms: i64, value: f64) -> datahub_datapoint {
    datahub_datapoint {
        timestamp_ms,
        value,
    }
}

/// Now, in epoch milliseconds. The spool's retention window is measured on each record's own
/// timestamp, so anything meant to survive in a spool has to be stamped with a recent time.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
