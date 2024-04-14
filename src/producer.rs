mod constants;

use kafka::producer::{Producer, Record, RequiredAcks};
use rand::Rng;
use std::fmt::Write;
use std::time::Duration;

fn main() {
    let mut producer = Producer::from_hosts(vec![constants::KAFKA_HOST.to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()
        .unwrap();

    let mut buf = String::with_capacity(2);
    for _i in 0..10 {
        let rand_number = rand::thread_rng().gen_range(1..=100);

        let _ = write!(&mut buf, "{}", rand_number); // some computation of the message data to be sent
        producer
            .send(&Record::from_value(constants::TOPIC_NAME, buf.as_bytes()))
            .unwrap();

        buf.clear();
    }
}
