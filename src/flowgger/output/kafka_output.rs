use super::Output;
use crate::flowgger::config::Config;
use crate::flowgger::merger::Merger;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::process::exit;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::thread;
use rdkafka::ClientConfig;
use futures::future::Future;
use std::collections::BTreeMap;
use toml::Value;

pub struct KafkaOutput {
    config: KafkaConfig
}

#[derive(Clone)]
struct KafkaConfig {
    topic: String,
    librdkafka: BTreeMap<String, Value>,
}

struct KafkaWorker {
    arx: Arc<Mutex<Receiver<Vec<u8>>>>,
    producer: FutureProducer,
    config: KafkaConfig,
}

impl KafkaWorker {
    fn new(arx: Arc<Mutex<Receiver<Vec<u8>>>>, config: KafkaConfig) -> KafkaWorker {
        let mut client_config = ClientConfig::new();
        for (k, v) in config.librdkafka.iter() {
            client_config.set(k, v.as_str().expect("All output.librdkafka settings MUST be strings even numbers"));
        }
        let producer: FutureProducer = client_config
            .create()
            .expect("Producer creation error");

        KafkaWorker {
            arx: arx,
            producer: producer,
            config: config,
        }
    }

    fn run(&mut self) {
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

}

impl KafkaOutput {
    pub fn new(config: &Config) -> KafkaOutput {
        let topic = config
            .lookup("output.topic").expect("output.topic must be a string")
            .as_str().expect("output.topic must be a string")
            .to_owned();
        let librdconfig = config
            .lookup("output.librdkafka").expect("output.librdkafka must be set")
            .as_table().expect("output.librdkafka must be set")
            .to_owned();
        KafkaOutput {
            config: KafkaConfig {
                topic: topic,
                librdkafka: librdconfig,
            }
        }
    }
}

impl Output for KafkaOutput {
    fn start(&self, arx: Arc<Mutex<Receiver<Vec<u8>>>>, merger: Option<Box<Merger>>) {
        if merger.is_some() {
            error!("Output framing is ignored with the Kafka output");
        }
        let tarx = Arc::clone(&arx);
        let tconfig = self.config.clone();
        thread::spawn(move || {
            let mut worker = KafkaWorker::new(tarx, tconfig);
            worker.run();
        });
    }
}
