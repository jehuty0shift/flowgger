use super::Input;
use crate::flowgger::config::Config;
use crate::flowgger::decoder::Decoder;
use crate::flowgger::encoder::Encoder;
use redis;
use redis::{Commands, Connection, RedisResult};
use std::process::exit;
use std::sync::mpsc::SyncSender;
use std::thread;

const DEFAULT_CONNECT: &'static str = "127.0.0.1";
const DEFAULT_QUEUE_KEY: &'static str = "logs";
const DEFAULT_THREADS: u32 = 1;

pub struct RedisInput {
    config: RedisConfig,
    threads: u32,
}

struct RedisWorker {
    tid: u32,
    config: RedisConfig,
    redis_cnx: Connection,
    tx: SyncSender<Vec<u8>>,
    decoder: Box<Decoder + Send>,
    encoder: Box<Encoder + Send>,
}

#[derive(Clone)]
struct RedisConfig {
    connect: String,
    queue_key: String,
}

impl RedisInput {
    pub fn new(config: &Config) -> RedisInput {
        let connect = config
            .lookup("input.redis_connect")
            .map_or(DEFAULT_CONNECT, |x| {
                x.as_str()
                    .expect("input.redis_connect must be an ip:port string")
            }).to_owned();
        let queue_key = config
            .lookup("input.redis_queue_key")
            .map_or(DEFAULT_QUEUE_KEY, |x| {
                x.as_str().expect("input.redis_queue_key must be a string")
            }).to_owned();
        let threads = config
            .lookup("input.redis_threads")
            .map_or(DEFAULT_THREADS, |x| {
                x.as_integer()
                    .expect("input.redis_threads must be a 32-bit integer") as u32
            });
        let redis_config = RedisConfig {
            connect: connect,
            queue_key: queue_key,
        };
        RedisInput {
            config: redis_config,
            threads: threads,
        }
    }
}

impl RedisWorker {
    fn new(
        tid: u32,
        config: RedisConfig,
        tx: SyncSender<Vec<u8>>,
        decoder: Box<Decoder + Send>,
        encoder: Box<Encoder + Send>,
    ) -> RedisWorker {
        let redis_cnx = match redis::Client::open(format!("redis://{}/", config.connect).as_ref()) {
            Err(_) => panic!(
                "Invalid connection string for the Redis server: [{}]",
                config.connect
            ),
            Ok(client) => match client.get_connection() {
                Err(_) => panic!(
                    "Unable to connect to the Redis server: [{}]",
                    config.connect
                ),
                Ok(redis_cnx) => redis_cnx,
            },
        };
        RedisWorker {
            tid: tid,
            config: config,
            redis_cnx: redis_cnx,
            tx: tx,
            decoder: decoder,
            encoder: encoder,
        }
    }

    fn run(self) -> Result<(), String> {
        let queue_key: &str = &self.config.queue_key;
        let queue_key_tmp: &str = &format!("{}.tmp.{}", queue_key, self.tid);
        let redis_cnx = self.redis_cnx;
        debug!(
            "Connected to Redis [{}], pulling messages from key [{}]",
            self.config.connect, queue_key
        );
        while {
            let dummy: RedisResult<String> = redis_cnx.rpoplpush(queue_key_tmp, queue_key);
            dummy.is_ok()
        } {}
        let (decoder, encoder): (Box<Decoder>, Box<Encoder>) = (self.decoder, self.encoder);
        loop {
            let line: String = match redis_cnx.brpoplpush(queue_key, queue_key_tmp, 0) {
                Err(e) => return Err(format!("Redis protocol error in BRPOPLPUSH: [{}]", e)),
                Ok(line) => line,
            };
            if let Err(e) = handle_record(&line, &self.tx, &decoder, &encoder) {
                error!("{}: [{}]", e, line.trim());
            }
            let res: RedisResult<u8> = redis_cnx.lrem(queue_key_tmp as &str, 1, line as String);
            if let Err(e) = res {
                return Err(format!("Redis protocol error in LREM: [{}]", e));
            };
        }
    }
}

impl Input for RedisInput {
    fn accept(
        &self,
        tx: SyncSender<Vec<u8>>,
        decoder: Box<Decoder + Send>,
        encoder: Box<Encoder + Send>,
    ) {
        let mut jids = Vec::new();
        for tid in 0..self.threads {
            let config = self.config.clone();
            let (encoder, decoder) = (encoder.clone_boxed(), decoder.clone_boxed());
            let tx = tx.clone();
            jids.push(thread::Builder::new().name(format!("redis-{}",tid)).spawn(move || {
                let worker = RedisWorker::new(tid, config, tx, decoder, encoder);
                if let Err(e) = worker.run() {
                    error!("Redis connection lost, aborting - {}", e);
                }
                exit(1);
            }).unwrap());
        }
        for jid in jids {
            if jid.join().is_err() {
                panic!("Redis connection lost");
            }
        }
    }
}

fn handle_record(
    line: &String,
    tx: &SyncSender<Vec<u8>>,
    decoder: &Box<Decoder>,
    encoder: &Box<Encoder>,
) -> Result<(), &'static str> {
    let decoded = decoder.decode(line)?;
    let reencoded = encoder.encode(decoded)?;
    tx.send(reencoded).unwrap();
    Ok(())
}
