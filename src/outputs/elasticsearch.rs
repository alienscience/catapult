use serde_json;
use serde_json::Value;
use serde_json::ser;

use hyper::{Client, Method, Request};
use hyper::header::{ContentLength, ContentType};
use futures::{Future, Stream};
use tokio_core::reactor::Core;

use processor::{ConfigurableFilter, OutputProcessor};

use std;
use std::str;
use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::thread::JoinHandle;

use filters::{time_to_index_name, transform};

pub struct Elasticsearch {
    name: String,
}

impl Elasticsearch {
    pub fn new(name: String) -> Elasticsearch {
        Elasticsearch { name: name }
    }
}

impl ConfigurableFilter for Elasticsearch {
    fn human_name(&self) -> &str {
        self.name.as_ref()
    }

    fn mandatory_fields(&self) -> Vec<&str> {
        vec!["destination", "port"]
    }
}

impl OutputProcessor for Elasticsearch {
    fn start(
        &self,
        rx: Receiver<String>,
        config: &Option<HashMap<String, String>>,
    ) -> Result<JoinHandle<()>, String> {
        self.requires_fields(config, self.mandatory_fields());
        self.invoke(rx, config, Elasticsearch::handle_func)
    }
    fn handle_func(rx: Receiver<String>, oconfig: Option<HashMap<String, String>>) {
        let config = oconfig.expect("Need a configuration");

        let destination_ip = config
            .get("destination")
            .expect("Need a destination IP")
            .clone();
        let destination_port = config
            .get("port")
            .expect("Need a destination port")
            .parse::<u32>()
            .unwrap();
        let mut core = Core::new().unwrap();

        loop {
            match rx.recv() {
                Ok(l) => match serde_json::from_str::<Value>(l.as_ref()) {
                    Ok(decoded) => {
                        let mut mutable_decoded = decoded;
                        let transformed = transform(&mut mutable_decoded);

                        println!("{:?}", transformed);

                        let index_name: Option<String> = match transformed.get("@timestamp") {
                            Some(time) => match time.as_str() {
                                Some(t) => Some(time_to_index_name(t)),
                                None => {
                                    error!("Failed to stringify.");
                                    None
                                }
                            },
                            None => {
                                error!("Failed to find timestamp.");
                                None
                            }
                        };

                        if !index_name.is_some() {
                            continue;
                        }

                        let index_name = index_name.unwrap();

                        let typ = "doc";
                        let output = ser::to_string(&transformed).ok().unwrap();
                        let mut client = Client::new(&core.handle());

                        let url = format!(
                            "http://{}:{}/{}/{}?op_type=index",
                            destination_ip,
                            destination_port,
                            index_name,
                            typ
                        );

                        let uri = url.parse().ok().expect("malformed url");

                        println!("Posting to elasticsearch with url: {}", url);

                        println!("Body = {}", output);
                        let body = output.into_bytes();
                        let mut req = Request::new(Method::Post, uri);
                        req.headers_mut().set(ContentType::json());
                        req.headers_mut().set(ContentLength(body.len() as u64));
                        req.set_body(body);
                        let work = client.request(req).and_then(|res| {
                            let status = res.status();
                            res.body().concat2().map(move |body| {
                                if !status.is_success() {
                                    println!(
                                        "Error response: {}",
                                        str::from_utf8(&*body).unwrap_or("")
                                    );
                                }
                            })
                        });
                        if let Err(msg) = core.run(work) {
                            println!("Cannot write to elasticsearch: {}", msg)
                        }
                    }
                    Err(s) => println!("Unable to parse line: {}\nfor {:?}", s, l),
                },
                Err(std::sync::mpsc::RecvError) => break,
            }
        }
    }
}
