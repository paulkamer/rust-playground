mod constants;

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

fn main() {
    let mut consumer = Consumer::from_hosts(vec![constants::KAFKA_HOST.to_owned()])
        .with_topic_partitions(constants::TOPIC_NAME.to_owned(), &[0, 1, 2])
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .unwrap();

    loop {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                let val = m.value.to_vec();

                let value = match String::from_utf8(val) {
                    Ok(v) => v,
                    Err(_) => "?".to_string(),
                };

                println!("{:?}", value);
            }

            let _ = consumer.consume_messageset(ms);
        }

        consumer.commit_consumed().unwrap();
    }
}
