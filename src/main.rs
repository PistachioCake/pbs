#![feature(new_uninit, maybe_uninit_array_assume_init, maybe_uninit_slice)]

use std::{
    cmp::Ordering,
    mem::{size_of, MaybeUninit},
};

use pbs::{
    lcg::LCG,
    scheduler::{Scheduler, MAX_LEVEL_SPLIT, SLICE_SIZE_BYTES},
    splitters::ScalarSplitter,
};

// number of u64 in one GB (power of two)
const BUF_SIZE_BYTES: usize = 1 << 30;
const BUF_SIZE: usize = BUF_SIZE_BYTES / size_of::<u64>();

fn main() {
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
    let mut buf: &mut [u64] = {
        // must have at least SLICE_SIZE_BYTES alignment!
        let ptr = unsafe {
            std::alloc::alloc(
                std::alloc::Layout::from_size_align(BUF_SIZE_BYTES, SLICE_SIZE_BYTES)
                    .expect("BUF_SIZE_BYTES and SLICE_SIZE_BYTES should be powers of two"),
            )
        } as *mut MaybeUninit<_>;
        let buf = unsafe { std::slice::from_raw_parts_mut(ptr, BUF_SIZE) };
        for el in buf.iter_mut() {
            el.write(lcg.next());
        }
        unsafe { MaybeUninit::slice_assume_init_mut(buf) }
    };

    let mut output: &mut [u64] = {
        // must have at least SLICE_SIZE_BYTES alignment!
        let ptr = unsafe {
            std::alloc::alloc(
                std::alloc::Layout::from_size_align(BUF_SIZE_BYTES, SLICE_SIZE_BYTES)
                    .expect("BUF_SIZE_BYTES and SLICE_SIZE_BYTES should be powers of two"),
            )
        } as *mut MaybeUninit<_>;
        let buf = unsafe { std::slice::from_raw_parts_mut(ptr, BUF_SIZE) };
        for el in buf.iter_mut() {
            el.write(0);
        }
        unsafe { MaybeUninit::slice_assume_init_mut(buf) }
    };

    let mut sched = Scheduler::new();
    let mut splitter = ScalarSplitter::default();

    eprintln!("Splitting");
    sched.split(&mut buf, &mut output, &mut splitter);
    eprintln!("Done splitting");

    let splits = sched.get_splits();

    assert!(check_split(output, 8 * MAX_LEVEL_SPLIT));
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
