use super::Output;
use crate::flowgger::config::Config;
use crate::flowgger::merger::Merger;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::process::exit;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use rdkafka::ClientConfig;
use futures::future::Future;

const KAFKA_DEFAULT_ACKS: i16 = 0;
const KAFKA_DEFAULT_COALESCE: usize = 1;
const KAFKA_DEFAULT_COMPRESSION: &'static str = "none";
const KAFKA_DEFAULT_THREADS: u32 = 1;
const KAFKA_DEFAULT_TIMEOUT: u64 = 60_000;

#[derive(Clone)]
pub enum Compression {
    NONE,
    GZIP,
    SNAPPY,
    LZ4,
    ZSTD,
}

impl Compression {
    pub fn as_str(&self) -> &'static str {
        match *self {
            Compression::NONE => "none",
            Compression::GZIP => "gzip",
            Compression::SNAPPY => "snappy",
            Compression::LZ4 => "lz4",
            Compression::ZSTD => "zstd",
        }
    }
    pub fn from_string(data: &str) -> Compression {
        match data {
            "none" => Compression::NONE,
            "gzip" => Compression::GZIP,
            "snappy" => Compression::SNAPPY,
            "lz4" => Compression::LZ4,
            "zstd" => Compression::ZSTD,
            _ => panic!("Unsupported compression method")
        }
    }
}

pub struct KafkaOutput {
    config: KafkaConfig,
    threads: u32,
}

#[derive(Clone)]
struct KafkaConfig {
    acks: i16,
    brokers: Vec<String>,
    topic: String,
    timeout: Duration,
    coalesce: usize,
    compression: Compression,
    flush_interval: Option<Duration>,
}

struct KafkaWorker {
    arx: Arc<Mutex<Receiver<Vec<u8>>>>,
    producer: FutureProducer,
    config: KafkaConfig,
    queue: Vec<String>,
    last_send: Instant,
}

impl KafkaWorker {
    fn new(arx: Arc<Mutex<Receiver<Vec<u8>>>>, config: KafkaConfig) -> KafkaWorker {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers.clone().join(","))
            .set("produce.offset.report", "true")
            .set("message.timeout.ms", &format!("{}", config.timeout.as_millis()))
            .set("request.required.acks", &format!("{}", config.acks))
            .set("compression.codec", config.compression.as_str())
            .create()
            .expect("Producer creation error");

        let queue = Vec::with_capacity(config.coalesce);
        KafkaWorker {
            arx: arx,
            producer: producer,
            config: config,
            queue: queue,
            last_send: Instant::now(),
        }
    }

    fn run_nocoalesce(&mut self) {
        loop {
            let bytes = match { self.arx.lock().unwrap().recv() } {
                Ok(line) => line,
                Err(_) => return,
            };
            let future = self.producer.send::<Vec<u8>, Vec<u8>>(FutureRecord::to(&self.config.topic).payload(&bytes), 0)
                .map(move |delivery_status| {
                    trace!("Delivery status for message received");
                    delivery_status
                });
            match future.wait() {
                Ok(data) => trace!("Future completed: {:?}", data),
                Err(e) => {
                    error!("Kafka not responsive: [{:?}]", e);
                    exit(1);
                }
            }
        }
    }

    fn run_coalesce(&mut self) {
        loop {
            let data = match { self.arx.lock().unwrap().recv() } {
                Ok(line) => match String::from_utf8(line) {
                    Ok(data) => data,
                    Err(_) => return,
                },
                Err(_) => return,
            };
            self.queue.push(data);
            trace!("Queue size: {}", self.queue.len());
            if self.queue.len() >= self.config.coalesce {
                debug!("coalesce reached!");
                let futures = self.queue.iter().map(|data| {
                    let rec = FutureRecord::to(&self.config.topic).payload(&data);
                    self.producer.send::<&String, &String>(rec, 0)
                        .map(move |delivery_status| {
                            trace!("Delivery status for message received");
                            delivery_status
                        })
                }).collect::<Vec<_>>();
                for future in futures {
                    match future.wait() {
                        Ok(data) => trace!("Future completed: {:?}", data),
                        Err(e) => {
                            error!("Kafka not responsive: [{:?}]", e);
                            exit(1);
                        }
                    }
                }
                self.queue.clear();
                self.last_send = Instant::now();
            } else if self.config.flush_interval.is_some() == true {
                if Instant::now().duration_since(self.last_send) > self.config.flush_interval.unwrap() {
                    debug!("flush_interval reached!");
                    let futures = self.queue.iter().map(|data| {
                        let rec = FutureRecord::to(&self.config.topic).payload(&data);
                        self.producer.send::<&String, &String>(rec, 0)
                            .map(move |delivery_status| {
                                trace!("Delivery status for message received");
                                delivery_status
                            })
                    }).collect::<Vec<_>>();
                    for future in futures {
                        match future.wait() {
                            Ok(data) => trace!("Future completed: {:?}", data),
                            Err(e) => {
                                error!("Kafka not responsive: [{:?}]", e);
                                exit(1);
                            }
                        }
                    }
                    self.queue.clear();
                    self.last_send = Instant::now();
                }
            }
        }
    }

    fn run(&mut self) {
        if self.config.coalesce <= 1 {
            self.run_nocoalesce()
        } else {
            self.run_coalesce()
        }
    }
}

impl KafkaOutput {
    pub fn new(config: &Config) -> KafkaOutput {
        let acks = config
            .lookup("output.kafka_acks")
            .map_or(KAFKA_DEFAULT_ACKS, |x| {
                x.as_integer()
                    .expect("output.kafka_acks must be a 16-bit integer") as i16
            });
        let brokers = config
            .lookup("output.kafka_brokers")
            .expect("output.kafka_brokers is required")
            .as_slice()
            .expect("Invalid list of Kafka brokers")
            .to_vec();
        let brokers = brokers
            .iter()
            .map(|x| {
                x.as_str()
                    .expect("output.kafka_brokers must be a list of strings")
                    .to_owned()
            }).collect();
        let topic = config
            .lookup("output.kafka_topic")
            .expect("output.kafka_topic must be a string")
            .as_str()
            .expect("output.kafka_topic must be a string")
            .to_owned();
        let timeout = Duration::from_millis(config.lookup("output.kafka_timeout").map_or(
            KAFKA_DEFAULT_TIMEOUT,
            |x| {
                x.as_integer()
                    .expect("output.kafka_timeout must be a 64-bit integer") as u64
            },
        ));
        let threads = config
            .lookup("output.kafka_threads")
            .map_or(KAFKA_DEFAULT_THREADS, |x| {
                x.as_integer()
                    .expect("output.kafka_threads must be a 32-bit integer") as u32
            });
        let coalesce = config
            .lookup("output.kafka_coalesce")
            .map_or(KAFKA_DEFAULT_COALESCE, |x| {
                x.as_integer()
                    .expect("output.kafka_coalesce must be a size integer") as usize
            });
        let flush_interval = match config.lookup("output.kafka_flush_interval") {
            Some(data) => Some(Duration::from_millis(data.as_integer().expect("output.kafka_flush_interval must be a size integer") as u64)),
            None => None
        };
        let compression = Compression::from_string(config
            .lookup("output.kafka_compression")
            .map_or(KAFKA_DEFAULT_COMPRESSION, |x| {
                x.as_str()
                    .expect("output.kafka_compresion must be a string")
            }).to_lowercase().as_ref()
        );
        let kafka_config = KafkaConfig {
            acks: acks,
            brokers: brokers,
            topic: topic,
            timeout: timeout,
            coalesce: coalesce,
            compression: compression,
            flush_interval: flush_interval,
        };
        KafkaOutput {
            config: kafka_config,
            threads: threads,
        }
    }
}

impl Output for KafkaOutput {
    fn start(&self, arx: Arc<Mutex<Receiver<Vec<u8>>>>, merger: Option<Box<Merger>>) {
        if merger.is_some() {
            error!("Output framing is ignored with the Kafka output");
        }
        for _ in 0..self.threads {
            let arx = Arc::clone(&arx);
            let config = self.config.clone();
            thread::spawn(move || {
                let mut worker = KafkaWorker::new(arx, config);
                worker.run();
            });
        }
    }
}
