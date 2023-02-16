use crate::scheduler::{ActiveSlices, Scheduler, Splitter, SplittingBucket};

#[derive(Default)]
pub struct ScalarSplitter;

impl<'a> Splitter<'a> for ScalarSplitter {
    fn split(
        &mut self,
        input: &[u64],
        shift: u8,
        mask: u64,
        output: &mut ActiveSlices<'a>,
        bucket: &mut SplittingBucket<'a>,
        sched: &mut Scheduler<'a>,
    ) {
        let mut num_elems = output.total_lens_of_buckets();
        for &key in input {
            let ix = (key >> shift) & mask;
            output.insert_element(bucket, sched, key, ix as usize);
            debug_assert_eq!(output.total_lens_of_full_buckets(bucket), num_elems + 1);
            num_elems += 1;
        }
    }

    fn split_small(&mut self, input: &[u64], output: &mut [u64]) {
        assert_eq!(input.len(), output.len());
        output.copy_from_slice(input);
        output.sort();
    }
}
