extern crate rusoto_core;
extern crate rusoto_kinesis;

use rusoto_core::{Region, HttpClient};
use rusoto_core::credential::ProfileProvider;
use rusoto_kinesis::{KinesisClient, PutRecordsRequestEntry, PutRecordsInput, Kinesis};
use std::io::{BufRead, BufReader};
use bytes::Bytes;
use uuid::Uuid;
use std::io;

#[tokio::main]
async fn main() {
    let stream_name = "some_stream_name".to_string();
    let kinesis_client = create_kinesis_client(String::from("profile_name"), Region::UsEast1);
    publish_messages_from_stdin(stream_name, kinesis_client).await;
}


fn create_kinesis_client(
    profile_name: String,
    region: Region,
) -> KinesisClient {
    let dispatcher = HttpClient::new().unwrap();
    let mut profile_provider = ProfileProvider::new().unwrap();
    profile_provider.set_profile(profile_name);
    KinesisClient::new_with(dispatcher, profile_provider, region)
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
            let _result = kinesis_client.put_records(
                PutRecordsInput {
                    records: record_list.clone(),
                    stream_name: stream_name.clone(),
                });
            // results_list.borrow().put(result);
            record_list.clear();
        }
    }
    let result = kinesis_client.put_records(
        PutRecordsInput {
            records: record_list.clone(),
            stream_name: stream_name.clone(),
        });
    let actual_result = result.await.unwrap();
    println!("failed count: {}", actual_result.failed_record_count.unwrap());
}