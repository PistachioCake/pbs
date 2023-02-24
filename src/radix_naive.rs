/// Goal: we should be able to replace Vec with our Slice type, passing in a SliceMgr, and have everything "just work"

/// A naive radix sort, using resizing Vectors.
pub fn radix_sort(input: &mut Box<[u64]>) {
    // TODO: use lens explicitly? Currently each stackframe contains its own len array
    // let mut lens: [[usize; 256]; 8] = [[0; 256]; 8];
    const EMPTY_BUCKET: Vec<u64> = Vec::new();
    let mut buckets: [Vec<u64>; 256] = [EMPTY_BUCKET; 256];
    for bucket in buckets.iter_mut() {
        bucket.reserve(((input.len() / 256) as f64 * 1.2) as usize);
    }
    let input_len = input.len();

    // L0 split
    for &key in input.iter() {
        let buck = (key >> 56) as usize;
        buckets[buck].push(key);
    }

    eprintln!("Finished L0");

    // we've already seen and copied all the keys from input, so we can reuse this memory
    let mut output = {
        let mut aux = [0; 0].into();
        std::mem::swap(&mut aux, input);

        Vec::from(aux)
    };
    output.clear(); // does not change capacity!

    debug_assert_eq!(input_len, output.capacity());

    let mut spare_buckets = vec![Vec::with_capacity(32), Vec::with_capacity(32)];
    let mut next_input = Vec::with_capacity((2048 as f64 * 1.2) as usize);
    for buck in 0..256 {
        std::mem::swap(&mut buckets[buck], &mut next_input);
        radix_sort_helper(
            &next_input,
            &mut buckets,
            &mut spare_buckets,
            &mut output,
            2,
            (buck as u64) << 56,
        );
        std::mem::swap(&mut buckets[buck], &mut next_input);
    }

    // lens[0] = buckets.map(|bucket| bucket.len());

    debug_assert_eq!(input_len, output.len());
    debug_assert_eq!(input_len, output.capacity());
    // as long as capacity was not modified, Vec::into_boxed_slice will not do any copying

    std::mem::swap(&mut output.into_boxed_slice(), input);
}

fn radix_sort_helper(
    input: &[u64],
    buckets: &mut [Vec<u64>; 256],
    spare_buckets: &mut Vec<Vec<u64>>,
    output: &mut Vec<u64>,
    level: u8,
    bucket_id: u64,
) {
    // eprint!("\r{bucket_id:#018x}, Splitting L{level}");

    if input.len() <= 32 {
        // small array, base case
        // eprint!("; finishing {} elements", input.len());
        let start_ix = output.len();
        output.extend(input);
        // FIXME use sorting networks - preferably offloading sorting networks
        output[start_ix..].sort_unstable();
        debug_assert_eq!(start_ix + input.len(), output.len());
        return;
    }

    if level == 9 {
        // all keys are the same!
        // eprint!("; finishing");
        output.extend(input);
        return;
    }

    let shift = (8 - level) * 8;
    let mask = 0xFF;

    // save these to reset the ends of buckets
    let bucket_lens: [usize; 256] = buckets.each_ref().map(|bucket| bucket.len());

    // Split these buckets
    for &key in input {
        let buck = ((key >> shift) & mask) as usize;
        debug_assert_ne!(buckets[buck].len(), buckets[buck].capacity());
        buckets[buck].push(key);
    }

    debug_assert_eq!(
        buckets
            .iter()
            .zip(bucket_lens.iter())
            .map(|(bucket, &prev_len)| bucket.len() - prev_len)
            .sum::<usize>(),
        input.len()
    );

    let output_len_before = output.len();

    // NOTE: in principle, I want to be able to not move the Vec around at all. For every bucket, I want to:
    //   1. take the elements we've created in this level of splitting
    //   2. split them again into these same buckets
    //   3. recurse all the way down to level 8 (or an early stop)
    // But step 2 may require resizing the Vec. If we resize the Vec we're curently splitting on, our input
    // reference becomes dangling. So instead, we replace this bucket's Vec with an empty one, saving the current
    // bucket's Vec into input. This means input contains only those elements we created in this level of splitting.

    let mut saved_bucket = spare_buckets.pop().unwrap();
    for buck in 0..256 {
        let bucket_id = (bucket_id & !(0xFF << shift)) | ((buck as u64) << shift);

        // We want to reuse allocations as much as possible. saved_bucket may have some capacity, but is empty.
        // We can place this into buckets so we don't have to deallocate this one and allocate a new one.
        debug_assert!(saved_bucket.is_empty());
        debug_assert_ne!(saved_bucket.capacity(), 0);
        std::mem::swap(&mut buckets[buck], &mut saved_bucket);
        radix_sort_helper(
            &saved_bucket[bucket_lens[buck]..],
            buckets,
            spare_buckets,
            output,
            level + 1,
            bucket_id,
        );
        std::mem::swap(&mut buckets[buck], &mut saved_bucket);

        buckets[buck].truncate(bucket_lens[buck]);
    }
    spare_buckets.push(saved_bucket);

    debug_assert_eq!(output_len_before + input.len(), output.len());
}
