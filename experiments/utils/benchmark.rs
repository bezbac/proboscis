use std::{
    process::Command,
    sync::mpsc::{channel, TryRecvError},
    thread,
    time::{Duration, Instant},
};

use itertools::Itertools;

pub fn benchmark_function(function: &dyn Fn() -> ()) {
    let (tx, rx) = channel();
    let receiver = thread::spawn(move || loop {
        let output = Command::new("sh")
            .arg("-c")
            .arg("docker stats --no-stream")
            .output()
            .expect("failed to execute process");

        dbg!(output);

        thread::sleep(Duration::from_millis(100));
        match rx.try_recv() {
            Ok(_) | Err(TryRecvError::Disconnected) => {
                println!("Terminating.");
                break;
            }
            Err(TryRecvError::Empty) => {}
        }
    });

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

    let _ = tx.send(());
    receiver.join().expect("The receiver thread has panicked");
}
