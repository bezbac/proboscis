use futures::StreamExt;
use itertools::Itertools;
use shiplift::Docker;
use std::{
    sync::mpsc::{channel, TryRecvError},
    thread,
    time::{Duration, Instant},
};

pub fn with_docker_stats(container_id: &str, function: &dyn Fn() -> ()) {
    let cloned_container_id = container_id.to_string();

    let (tx, rx) = channel();
    let receiver = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let docker = Docker::new();
        let containers = docker.containers();

        loop {
            let output = rt.block_on(containers.get(&cloned_container_id).stats().next());

            // dbg!(output);

            thread::sleep(Duration::from_millis(100));
            match rx.try_recv() {
                Ok(_) | Err(TryRecvError::Disconnected) => {
                    println!("Terminating.");
                    break;
                }
                Err(TryRecvError::Empty) => {}
            }
        }
    });

    function();

    let _ = tx.send(());
    receiver.join().expect("The receiver thread has panicked");
}

pub fn benchmark_function(function: &dyn Fn() -> ()) {
    let iterations = 1000;
    let before_all = Instant::now();

    let mut milis = vec![];

    for _ in 0..iterations {
        let before_each = Instant::now();
        function();
        milis.push(before_each.elapsed().as_millis());
    }

    println!(
        "Total time ({} iterations): {:.2?}",
        iterations,
        before_all.elapsed()
    );

    println!(
        "Avg time: {:.2?}ms",
        milis.iter().sum1::<u128>().unwrap() / milis.len() as u128
    );

    println!("Min time: {:.2?}ms", milis.iter().min().unwrap());
    println!("Max time: {:.2?}ms", milis.iter().max().unwrap());
}
