use std::time::Instant;

use itertools::Itertools;

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
