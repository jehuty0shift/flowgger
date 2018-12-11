use super::Splitter;
use crate::flowgger::decoder::Decoder;
use crate::flowgger::encoder::Encoder;
use std::io::{BufRead, BufReader, ErrorKind, Read};
use std::str;
use std::sync::mpsc::SyncSender;

pub struct NulSplitter;

impl<T: Read> Splitter<T> for NulSplitter {
    fn run(
        &self,
        buf_reader: BufReader<T>,
        tx: SyncSender<Vec<u8>>,
        decoder: Box<Decoder>,
        encoder: Box<Encoder>,
    ) {
        for line in buf_reader.split(0) {
            let line = match line {
                Ok(line) => line,
                Err(e) => match e.kind() {
                    ErrorKind::Interrupted => continue,
                    ErrorKind::WouldBlock => {
                        warn!("Client hasn't sent any data for a while - Closing idle connection");
                        return;
                    }
                    _ => return,
                },
            };
            let line = match str::from_utf8(&line) {
                Err(_) => {
                    error!("Invalid UTF-8 input");
                    continue;
                }
                Ok(line) => line,
            };
            if let Err(e) = handle_line(line, &tx, &decoder, &encoder) {
                let line = line.trim();
                if !line.is_empty() {
                    error!("{}: [{}]", e, line.trim());
                }
            }
        }
    }
}

fn handle_line(
    line: &str,
    tx: &SyncSender<Vec<u8>>,
    decoder: &Box<Decoder>,
    encoder: &Box<Encoder>,
) -> Result<(), &'static str> {
    let decoded = decoder.decode(line)?;
    let reencoded = encoder.encode(decoded)?;
    tx.send(reencoded).unwrap();
    Ok(())
}
