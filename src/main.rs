extern crate rusoto_core;
extern crate rusoto_kinesis;

use bytes::Bytes;
use rusoto_core::credential::{ProfileProvider, ChainProvider};
use rusoto_core::{HttpClient, Region};
use rusoto_kinesis::{Kinesis, KinesisClient, PutRecordsInput, PutRecordsRequestEntry};
use std::io;
use std::io::{BufRead, BufReader};
use uuid::Uuid;
use clap::{App, Arg};

#[tokio::main]
async fn main() {
    // TODO: refactor this into it's own method
    let matches = App::new("Kinesis Cli Uploader")
        .arg(Arg::with_name("stream_name")
            .short("s")
            .long("stream-name")
            .value_name("STREAM_NAME")
            .help("The name of the stream")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("profile_name")
            .short("p")
            .long("profile")
            .value_name("AWS_PROFILE_NAME")
            .help("The aws profile name in the credentials file")
            .takes_value(true))
        .get_matches();
    let stream_name = matches.value_of("stream_name").unwrap().to_string();
    println!("using stream: {}", stream_name);

    let profile_name = matches.value_of("profile_name");
    let kinesis_client = create_kinesis_client(Region::UsEast1, profile_name);
    publish_messages_from_stdin(stream_name, kinesis_client).await;
}

fn create_kinesis_client(region: Region, profile_name: Option<&str>) -> KinesisClient {
    let dispatcher = HttpClient::new().unwrap();
    let credentials_provider = create_credentials_provider(profile_name);
    KinesisClient::new_with(dispatcher, credentials_provider, region)
}

fn create_credentials_provider(profile_name: Option<&str>) -> ChainProvider {
    match profile_name {
        Some(profile) => {
            let mut profile_provider = ProfileProvider::new().unwrap();
            profile_provider.set_profile(profile.to_string());
            return ChainProvider::with_profile_provider(profile_provider);
        }
        None => {
            return ChainProvider::new();
        }
    }
}

async fn publish_messages_from_stdin(stream_name: String, kinesis_client: KinesisClient) {
    let mut stdin = BufReader::new(io::stdin());
    let mut record_list: Vec<PutRecordsRequestEntry> = Vec::new();

    loop {
        let mut buff: Vec<u8> = Vec::new();
        let result = stdin.read_until(b"\n"[0], &mut buff);
        if result.unwrap() == 0 { break; }

        record_list.push(PutRecordsRequestEntry {
            data: Bytes::from(buff),
            explicit_hash_key: Option::None,
            partition_key: Uuid::new_v4().to_hyphenated().to_string(),
        });

        if record_list.len() > 100 {
            let _result = kinesis_client.put_records(PutRecordsInput {
                records: record_list.clone(),
                stream_name: stream_name.clone(),
            });
            record_list.clear();
        }
    }
    let result = kinesis_client.put_records(PutRecordsInput {
        records: record_list.clone(),
        stream_name: stream_name.clone(),
    });

    let actual_result = result.await.unwrap();
    println!(
        "failed count: {}",
        actual_result.failed_record_count.unwrap()
    );
}
