#![feature(new_uninit)]
#![feature(const_result_drop)]
#![feature(const_option)]

use std::{
    cmp::Ordering,
    mem::{size_of, MaybeUninit},
    time::Instant,
};

use pbs::{
    lcg::LCG,
    radix_naive::radix_sort,
    scheduler::{Scheduler, MAX_LEVEL_SPLIT, SLICE_SIZE_BYTES},
    splitters::ScalarSplitter,
};

// number of u64 in one GB (power of two)
const BUF_SIZE_BYTES: usize = 1 << 30;
const BUF_SIZE: usize = BUF_SIZE_BYTES / size_of::<u64>();

fn main() {
    _main_test2();
}

fn _main_test2() {
    let mut lcg = LCG::new();

    let buf = {
        let mut buf = Box::new_uninit_slice(BUF_SIZE);
        for el in buf.iter_mut() {
            el.write(lcg.next());
        }
        unsafe { buf.assume_init() }
    };

    let mut buf = std::hint::black_box(buf);

    eprintln!("Splitting");
    let start = Instant::now();

    radix_sort(&mut buf);

    let end = Instant::now();
    eprintln!("Done splitting");

    let secs = end.duration_since(start).as_secs_f64();
    let speed = (BUF_SIZE as f64 / (1 << 27) as f64) / secs;
    let item_speed = speed / size_of::<u64>() as f64;
    println!("Time: {secs:.2} s, Speed: {speed:.2} GB/s = {item_speed:.2} B keys/s");

    let buf = std::hint::black_box(buf);

    assert!(check_split(&buf, 64));
}

fn _main_test1() {
    const BUF_LAYOUT: std::alloc::Layout =
        std::alloc::Layout::from_size_align(BUF_SIZE_BYTES, SLICE_SIZE_BYTES)
            .ok()
            .expect("BUF_SIZE_BYTES and SLICE_SIZE_BYTES should be powers of two");
    let mut lcg = LCG::new();
    /*
     * THIS IS NOT PROPERLY ALIGNED!!
     **/
    // let mut buf = {
    //     let mut buf: Box<[MaybeUninit<u64>]> = Box::new_uninit_slice(BUF_SIZE);
    //     for el in buf.iter_mut() {
    //         el.write(lcg.next());
    //     }
    //     unsafe { buf.assume_init() }
    // };
    let buf: Box<[u64]> = {
        // must have at least SLICE_SIZE_BYTES alignment!
        let ptr = unsafe { std::alloc::alloc(BUF_LAYOUT) } as *mut MaybeUninit<_>;
        let mut buf = unsafe { Box::from_raw(std::slice::from_raw_parts_mut(ptr, BUF_SIZE)) };
        for el in buf.iter_mut() {
            el.write(lcg.next());
        }
        unsafe { buf.assume_init() }
    };

    let output: Box<[u64]> = {
        // must have at least SLICE_SIZE_BYTES alignment!
        let ptr = unsafe { std::alloc::alloc(BUF_LAYOUT) } as *mut MaybeUninit<_>;
        let mut buf = unsafe { Box::from_raw(std::slice::from_raw_parts_mut(ptr, BUF_SIZE)) };
        for el in buf.iter_mut() {
            el.write(0);
        }
        unsafe { buf.assume_init() }
    };

    let (mut buf, mut output) = std::hint::black_box((buf, output));

    let mut sched = Scheduler::new();
    let mut splitter = ScalarSplitter::default();

    eprintln!("Splitting");
    let start = Instant::now();

    sched.split(&mut buf, &mut output, &mut splitter);

    let end = Instant::now();
    eprintln!("Done splitting");

    let secs = end.duration_since(start).as_secs_f64();
    let speed = (BUF_SIZE as f64 / (1 << 27) as f64) / secs;
    let item_speed = speed / size_of::<u64>() as f64;
    println!("Time: {secs:.2} s, Speed: {speed:.2} GB/s = {item_speed:.2} B keys/s");

    let (buf, output) = std::hint::black_box((buf, output));

    // let splits = sched.get_splits();

    assert!(check_split(&output, 8 * MAX_LEVEL_SPLIT));

    // we cannot let the Box free its data, since we alloced the memory ourselves
    unsafe {
        std::alloc::dealloc(Box::<[u64]>::into_raw(buf) as *mut u8, BUF_LAYOUT);
        std::alloc::dealloc(Box::<[u64]>::into_raw(output) as *mut u8, BUF_LAYOUT);
    }
}

fn check_split(buf: &[u64], num_bits: u8) -> bool {
    let mask = if num_bits == 64 {
        !0
    } else {
        (1 << num_bits) - 1
    };
    let mask = mask << (64 - num_bits);
    let mut total = 0usize;
    let mut incorrect = 0usize;
    let mut current = 0;

    for (ix, key) in buf
        .iter() /*.flat_map(|slice| slice.iter())*/
        .enumerate()
    {
        let bucket = key & mask;
        total += 1;
        match current.cmp(&bucket) {
            Ordering::Less => {
                current = bucket;
            }
            Ordering::Equal => (),
            Ordering::Greater => {
                incorrect += 1;
                eprintln!("Failed at {ix}: {key:#x} should be in bucket {current:#x}");
            }
        }
    }

    if incorrect != 0 {
        eprintln!("{incorrect} failures");
    }

    if total != BUF_SIZE {
        eprintln!("Saw {total} elems, expected {BUF_SIZE}");
    }

    incorrect == 0 && total == BUF_SIZE
}
