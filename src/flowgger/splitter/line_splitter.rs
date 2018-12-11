use super::Splitter;
use crate::flowgger::decoder::Decoder;
use crate::flowgger::encoder::Encoder;
use std::io::{BufRead, BufReader, ErrorKind, Read};
use std::sync::mpsc::SyncSender;

pub struct LineSplitter;

impl<T: Read> Splitter<T> for LineSplitter {
    fn run(
        &self,
        buf_reader: BufReader<T>,
        tx: SyncSender<Vec<u8>>,
        decoder: Box<Decoder>,
        encoder: Box<Encoder>,
    ) {
        for line in buf_reader.lines() {
            let line = match line {
                Ok(line) => line,
                Err(e) => match e.kind() {
                    ErrorKind::Interrupted => continue,
                    ErrorKind::InvalidInput | ErrorKind::InvalidData => {
                        error!("Invalid UTF-8 input");
                        continue;
                    }
                    ErrorKind::WouldBlock => {
                        warn!("Client hasn't sent any data for a while - Closing idle connection");
                        return;
                    }
                    _ => return,
                },
            };
            if let Err(e) = handle_line(&line, &tx, &decoder, &encoder) {
                error!("{}: [{}]", e, line.trim());
            }
        }
    }
}

fn handle_line(
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
