use futures::StreamExt;
use shiplift::{rep::Stats, Docker as ShipliftDocker};
use std::{
    sync::mpsc::{channel, TryRecvError},
    thread,
    time::{Duration, Instant},
};

pub fn while_collecting_docker_stats<T: Sized>(
    container_id: &str,
    function: &dyn Fn() -> T,
) -> (Vec<(Instant, Stats)>, T) {
    let cloned_container_id = container_id.to_string();

    let (tx, rx) = channel();
    let receiver = thread::spawn(move || {
        let mut collection = vec![];

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let docker = ShipliftDocker::new();
        let containers = docker.containers();

        loop {
            let stats = rt
                .block_on(containers.get(&cloned_container_id).stats().next())
                .unwrap()
                .unwrap();

            collection.push((Instant::now(), stats));

            thread::sleep(Duration::from_millis(10));
            match rx.try_recv() {
                Ok(_) | Err(TryRecvError::Disconnected) => {
                    println!("Terminating.");
                    break;
                }
                Err(TryRecvError::Empty) => {}
            }
        }

        collection
    });

    let output = function();

    let _ = tx.send(());
    let stats = receiver.join().expect("The receiver thread has panicked");

    (stats, output)
}
