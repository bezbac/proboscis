use itertools::Itertools;
use std::time::Instant;

pub fn benchmark_function(
    iterations: i32,
    function: &dyn Fn(i32) -> (),
) -> Vec<(Instant, Instant)> {
    let mut collection = vec![];

    for i in 0..iterations {
        let before_each = Instant::now();
        function(i);
        collection.push((Instant::now(), before_each));
    }

    collection
}

pub fn print_benchmark_stats(times: &[(Instant, Instant)]) {
    let first_time = times.first().unwrap().1;
    let last_time = times.last().unwrap().0;
    let total_time = last_time.duration_since(first_time);

    println!("Total time: {:.2?}", total_time);

    let durations_in_milis: Vec<u128> = times
        .iter()
        .map(|(end, start)| end.duration_since(*start).as_millis())
        .collect();

    println!(
        "Avg time: {:.2?}ms",
        durations_in_milis.iter().sum1::<u128>().unwrap() / durations_in_milis.len() as u128
    );
    println!(
        "Min time: {:.2?}ms",
        durations_in_milis.iter().min().unwrap()
    );
    println!(
        "Max time: {:.2?}ms",
        durations_in_milis.iter().max().unwrap()
    );
}
