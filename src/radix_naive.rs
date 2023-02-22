/// Goal: we should be able to replace Vec with our Slice type, passing in a SliceMgr, and have everything "just work"

/// A naive radix sort, using resizing Vectors.
pub fn radix_sort(input: &mut Box<[u64]>) {
    // TODO: use lens explicitly? Currently each stackframe contains its own len array
    // let mut lens: [[usize; 256]; 8] = [[0; 256]; 8];
    const EMPTY_BUCKET: Vec<u64> = Vec::new();
    let mut buckets: [Vec<u64>; 256] = [EMPTY_BUCKET; 256];
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

    for buck in 0..256 {
        let mut input = Vec::new();
        std::mem::swap(&mut buckets[buck], &mut input);
        radix_sort_helper(&input, &mut buckets, &mut output, 2, (buck as u64) << 56);
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
        output[start_ix..].sort();
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
    let bucket_lens: [usize; 256] = buckets
        .iter()
        .map(|bucket| bucket.len())
        // TODO prevent allocation here? Maybe the compiler does it, but not sure
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();

    // Split these buckets
    for &key in input {
        let buck = ((key >> shift) & mask) as usize;
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
    
    // TODO: reserve some starting capacity here? Heuristically determine distribution?
    let mut saved_bucket = Vec::new();
    for buck in 0..256 {
        let bucket_id = (bucket_id & !(0xFF << shift)) | ((buck as u64) << shift);
        
        // We want to reuse allocations as much as possible. saved_bucket may have some capacity, but is empty.
        // We can place this into buckets so we don't have to deallocate this one and allocate a new one.
        debug_assert!(saved_bucket.is_empty());
        std::mem::swap(&mut buckets[buck], &mut saved_bucket);
        radix_sort_helper(&saved_bucket[bucket_lens[buck]..], buckets, output, level + 1, bucket_id);
        std::mem::swap(&mut buckets[buck], &mut saved_bucket);

        // TODO can maybe replace with Vec::set_len?
        saved_bucket.truncate(bucket_lens[buck]);
    }

    buckets
        .iter_mut()
        .zip(bucket_lens.iter())
        // TODO can maybe replace with Vec::set_len?
        .for_each(|(bucket, &len)| bucket.truncate(len));

    debug_assert_eq!(output_len_before + input.len(), output.len());
}
