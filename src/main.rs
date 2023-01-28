use std::{sync::{atomic::{AtomicI32, Ordering}}, time::{Instant, Duration}};

use anyhow::Error;
use clap::Parser;
use hdrhistogram::Histogram;
use rdkafka::{ClientConfig, consumer::{CommitMode, StreamConsumer, Consumer}, producer::{FutureProducer, FutureRecord}, Message, config::RDKafkaLogLevel};
use tokio::{sync::{Mutex}, task::yield_now};
use futures::{TryStreamExt};

/// open-loop load tester for scylla
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// kafka server to connect to. include the port number.
    #[arg(short, long)]
    server: String,

    /// the topic we should write to and read from
    #[arg(short, long)]
    topic: String,

    /// username if basic password auth should be used
    #[arg(short, long)]
    username: Option<String>,

    /// password if basic password auth should be used
    #[arg(short, long)]
    password: Option<String>,

    /// size of the value to write
    #[arg(short, long, default_value_t = 1024)]
    blob_size: u32,

    /// constant throughput to write at in requests per second
    #[arg(long)]
    rps: u64,

    /// duration of the test in seconds
    #[arg(long)]
    runtime_s: u64,

    /// upper limit on the concurrency to be used to reach desired rps
    #[arg(long, default_value_t = 4096)]
    max_concurrency: usize,
}

impl Args {
    async fn bench(&self) -> Result<(), Error> {
        let mut blob : Vec<u8> = Vec::new();
        if self.blob_size < 8 {
            panic!("payload size must be at least 8 to accommodate a timestamp");
        }
        
        for _i in 0..(self.blob_size) {
            blob.push(42_u8);
        }

        let concurrency = AtomicI32::new(0);

        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "test")
            .set("bootstrap.servers", &self.server)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "latest")
            .set("debug", "all")
            .set("security.protocol", "SASL_SSL")
            .set("sasl.mechanisms", "SCRAM-SHA-256")
            .set("sasl.username", self.username.as_ref().unwrap())
            .set("sasl.password", self.password.as_ref().unwrap())
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()
            .expect("Consumer creation failed");

        consumer
            .subscribe(&[&self.topic])
            .expect("Can't subscribe to specified topic");

        // Create the `FutureProducer` to produce asynchronously.
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.server)
            .set("message.timeout.ms", "5000")
            .set("debug", "all")
            .set("security.protocol", "SASL_SSL")
            .set("sasl.mechanisms", "SCRAM-SHA-256")
            .set("sasl.username", self.username.as_ref().unwrap())
            .set("sasl.password", self.password.as_ref().unwrap())
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()
            .expect("Producer creation error");

        // To avoid coordinated omission, we pre-assign a send time to every planned request at test start.
        // This send time is bench_start + delay_between_requests * send_index

        // If the SUT and test are keeping up then this assigned send time will be the real time. If the system
        // isn't keeping up and our max_concurrency is exhausted then send time and real time can arbitrarily diverge.
        // As long as max_concurrency was sufficient to drive the SUT out of its linear scalability region and the
        // test hardware was sufficient to provide max_concurrency, then this divergence is realistic (imagine that the
        // divergence represents requests stuck in the accept queue or waiting in kafka.)
        let bench_start = Instant::now();
        let delay_between_requests = Duration::from_nanos((1e9 / self.rps as f64) as u64);

        let mut spawns = 0i32;

        let (work_sender, work_receiver) = async_channel::bounded::<i32>(self.max_concurrency);

        tokio_scoped::scope(|s| {
            // result collector
            s.spawn(async {
                let hist : Mutex<Histogram<u64>> = Mutex::new(hdrhistogram::Histogram::new(3).unwrap());

                eprintln!("runtime_uS,concurrency");
                let last_print = Mutex::new(Instant::now());
                let result = consumer.stream().try_for_each(|message| {
                    let message = message.detach();
                    let concurrency = &concurrency;
                    let hist = &hist;
                    let last_print = &last_print;

                    async move {
                        if let Option::Some(message) = message.payload() {
                            let since_start = u64::from_be_bytes(message[0..8].try_into().unwrap());
                            if since_start == 0 {
                                // terminate
                                return std::result::Result::Err(rdkafka::error::KafkaError::Canceled);
                            }
                            let start_time = bench_start + Duration::from_nanos(since_start);
                            let latency = start_time.elapsed();
                            hist.lock().await.record(latency.as_nanos() as u64).unwrap();

                            let mut last_print = last_print.lock().await;
                            if last_print.elapsed().as_secs() >= 1 {
                                eprintln!("{},{}", bench_start.elapsed().as_micros(), concurrency.load(Ordering::Relaxed));
                                *last_print = Instant::now();
                            } 
                        }
                        std::result::Result::Ok(())
                    }
                }).await;

                //consumer.commit_consumer_state(CommitMode::Sync).unwrap();
                if let std::result::Result::Err(rdkafka::error::KafkaError::Canceled) = result {
                    let hist = hist.into_inner();
                    quantiles(&hist, 2, 3).unwrap();
                } else {
                    result.unwrap();
                }
            });

            // workers
            s.scope(|s| {

                for _i in 0..self.max_concurrency {
                    s.spawn(async {
                        // workers consume from the driver produced queue
                        while let Result::Ok(idx) = work_receiver.recv().await {
                            let since_start = (delay_between_requests.as_nanos() * idx as u128) as u64;

                            concurrency.fetch_add(1, Ordering::Relaxed);
                            let mut blob = blob.clone();

                            // pack time since bench start into our version of the blob
                            blob[0..8].copy_from_slice(since_start.to_be_bytes().as_slice());

                            let record : FutureRecord<String, Vec<u8>> = FutureRecord::to(&self.topic).payload(&blob);
                            producer.send(record, Duration::from_secs(10)).await.unwrap();
                            concurrency.fetch_add(-1, Ordering::Relaxed);
                        }
                    });
                }

                // spawn driver
                s.spawn(async {
                    while bench_start.elapsed().as_secs() < self.runtime_s {
                        let target_spawns = ((bench_start.elapsed().as_nanos() * self.rps as u128) as f64 / 1e9).floor() as i32;
                        let next_spawns = target_spawns - spawns;
        
                        for i in 0..next_spawns {
                            work_sender.send(spawns + i).await.unwrap();
                        }
                        spawns += next_spawns;

                        if next_spawns == 0 {
                            yield_now().await;
                        }
                    }
                    work_sender.close();
                });
            });

            s.spawn( async {
                // once the driver and workers shut down we emit the stop message
                let mut blob = blob.clone();

                // stop message is zero elapsed time
                blob[0..8].copy_from_slice(0u64.to_be_bytes().as_slice());

                let record : FutureRecord<String, Vec<u8>> = FutureRecord::to(&self.topic).payload(&blob);
                producer.send(record, Duration::from_secs(0)).await.unwrap();
            });
        });
        
        Result::Ok(())
    }
}


#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();
    let args : Args = Args::parse();
    args.bench().await?;
    
    Result::Ok(())
}

fn quantiles(
    hist: &Histogram<u64>,
    quantile_precision: usize,
    ticks_per_half: u32,
) -> Result<(), Error> {
    println!(
        "{:>12} {:>quantile_precision$} {:>quantile_precision$} {:>10} {:>14}",
        "Value",
        "QuantileValue",
        "QuantileIteration",
        "TotalCount",
        "1/(1-Quantile)",
        quantile_precision = quantile_precision + 2 // + 2 from leading "0." for numbers
    );
    let mut sum = 0;
    for v in hist.iter_quantiles(ticks_per_half) {
        sum += v.count_since_last_iteration();
        if v.quantile_iterated_to() < 1.0 {
            println!(
                "{:12} {:1.*} {:1.*} {:10} {:14.2}",
                v.value_iterated_to(),
                quantile_precision,
                v.quantile(),
                quantile_precision,
                v.quantile_iterated_to(),
                sum,
                1_f64 / (1_f64 - v.quantile_iterated_to())
            );
        } else {
            println!(
                "{:12} {:1.*} {:1.*} {:10} {:>14}",
                v.value_iterated_to(),
                quantile_precision,
                v.quantile(),
                quantile_precision,
                v.quantile_iterated_to(),
                sum,
                "∞"
            );
        }
    }
        
    Result::Ok(())
}
