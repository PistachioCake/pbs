use std::alloc::Layout;
use std::marker::PhantomData;
use std::mem::{size_of, swap};

pub const SLICE_SIZE_BYTES: usize = 0x10000;
pub const SLICE_SIZE: usize = SLICE_SIZE_BYTES / size_of::<u64>();
pub const NUM_BUCKETS: usize = 256;
pub const MAX_LEVEL_SPLIT: u8 = 8;

pub struct SplittingBucket<'a> {
    pub children: Box<[UnsplitBucket<'a>; NUM_BUCKETS]>,
}

pub struct SplitBucket<'a> {
    pub children: Box<[Bucket<'a>; NUM_BUCKETS]>,
}

#[derive(Default)]
pub struct UnsplitBucket<'a> {
    // read-only but unique
    pub slices: Vec<&'a mut [u64]>,
}

pub enum Bucket<'a> {
    Split(SplitBucket<'a>),
    Unsplit(UnsplitBucket<'a>),
    Sorted,
}

pub struct ActiveSlices<'a> {
    ptrs: Box<[*mut u64; NUM_BUCKETS]>,
    phantom: PhantomData<&'a mut u64>,
}

pub struct Scheduler<'a> {
    free_slices: Vec<*mut u64>,
    top_level: Option<Bucket<'a>>,
    phantom: PhantomData<&'a mut u64>,
}

impl<'a> Scheduler<'a> {
    fn free_slice<'b>(&'b mut self, slice: &'b mut [u64]) {
        let ptr = slice.as_mut_ptr();
        assert!(ptr.is_aligned_to(SLICE_SIZE_BYTES));
        self.free_slices.push(ptr);
    }

    fn get_slice(&mut self) -> *mut u64 {
        if let Some(ptr) = self.free_slices.pop() {
            debug_assert!(ptr.is_aligned_to(SLICE_SIZE_BYTES));
            return ptr;
        }

        const LAYOUT: Layout = Layout::from_size_align(SLICE_SIZE_BYTES, SLICE_SIZE_BYTES)
            .ok()
            .unwrap();

        let ptr = unsafe { std::alloc::alloc(LAYOUT) as *mut u64 };

        debug_assert!((ptr as usize & (SLICE_SIZE_BYTES - 1)) == 0);

        if ptr.is_null() {
            // handle_alloc_error(LAYOUT);
            panic!("Could not allocate new free slice");
        }
        ptr
    }
}

impl<'a> Default for ActiveSlices<'a> {
    fn default() -> Self {
        Self {
            ptrs: Box::new([std::ptr::null_mut(); NUM_BUCKETS]),
            phantom: PhantomData,
        }
    }
}

impl<'a> ActiveSlices<'a> {
    fn len_of_ptr(ptr: *mut u64) -> usize {
        if ptr.is_null() {
            return 0;
        }
        let offset = (ptr as usize & (SLICE_SIZE_BYTES - 1)) / size_of::<u64>();
        if offset == 0 {
            SLICE_SIZE
        } else {
            offset
        }
    }

    pub fn len_of_bucket(&self, ix: usize) -> usize {
        Self::len_of_ptr(self.ptrs[ix])
    }

    pub fn total_lens_of_buckets(&self) -> usize {
        (0..256).map(|ix| self.len_of_bucket(ix)).sum()
    }

    pub fn total_lens_of_full_buckets(&self, bucket: &SplittingBucket<'a>) -> usize {
        bucket
            .children
            .iter()
            .flat_map(|child| &child.slices)
            .map(|slice| slice.len())
            .sum::<usize>()
            + self.total_lens_of_buckets()
    }

    pub fn insert_element(
        &mut self,
        bucket: &mut SplittingBucket<'a>,
        sched: &mut Scheduler,
        el: u64,
        ix: usize,
    ) {
        let ptr = &mut self.ptrs[ix];
        if ptr.is_aligned_to(SLICE_SIZE_BYTES) {
            // we are at the end of a slice, so we cannot append here
            if !ptr.is_null() {
                // put this slice into the child bucket, and get a new slice
                let slice = unsafe {
                    // reset pointer to start of slice
                    let start_ptr = ptr.sub(SLICE_SIZE);
                    // dbg!(("full", start_ptr, &*ptr, ix));
                    std::slice::from_raw_parts_mut(start_ptr, SLICE_SIZE)
                };
                bucket.children[ix].slices.push(slice);
            }

            *ptr = sched.get_slice();
        }

        unsafe { ptr.write(el) }
        *ptr = unsafe { ptr.add(1) };
    }

    pub fn insert_elements(
        &mut self,
        bucket: &mut SplittingBucket<'a>,
        sched: &mut Scheduler,
        els: &[u64],
        ix: usize,
    ) {
        if self.len_of_bucket(ix) >= els.len() {
            // we have enough space, just insert them all
            // happy path!
            let ptr = &mut self.ptrs[ix];
            unsafe { ptr.copy_from_nonoverlapping(els.as_ptr(), els.len()) };
            *ptr = unsafe { ptr.add(els.len()) };
            return;
        }
        // if not, we need to get a new slice *somewhere* in this insert
        // TODO we can do this with a copy, getting a new slice, and then another copy
        for &el in els {
            self.insert_element(bucket, sched, el, ix);
        }
    }

    pub fn complete(self, bucket: &mut SplittingBucket<'a>) {
        for (ptr, child) in self.ptrs.into_iter().zip(bucket.children.iter_mut()) {
            if !ptr.is_null() {
                let slice = unsafe {
                    let els_in_slice = Self::len_of_ptr(ptr);
                    let start_ptr = ptr.sub(els_in_slice);
                    debug_assert!(els_in_slice <= SLICE_SIZE);
                    // dbg!(("partial", start_ptr, ptr, els_in_slice /*,idx*/,));
                    std::slice::from_raw_parts_mut(start_ptr, els_in_slice)
                };
                child.slices.push(slice);
            }
        }
    }
}

impl<'a> Default for SplittingBucket<'a> {
    fn default() -> Self {
        // we would like to use #[derive(Default)] on SplittingBucket, but we don't have
        // `[T; 256]: Default where T: Default`
        const CHILD: UnsplitBucket = UnsplitBucket { slices: vec![] };
        Self {
            children: Box::new([CHILD; 256]),
        }
    }
}

impl<'a> UnsplitBucket<'a> {
    fn split(
        self,
        sched: &mut Scheduler<'a>,
        splitter: &mut dyn Splitter<'a>,
        shift: u8,
        mask: u64,
    ) -> SplittingBucket<'a> {
        let slices = self.slices;
        let mut dests = ActiveSlices::default();
        let mut res = SplittingBucket::default();

        let mut num_els_split = 0;

        debug_assert_eq!(dests.total_lens_of_buckets(), 0);

        for slice in slices {
            splitter.split(slice, shift, mask, &mut dests, &mut res, sched);

            debug_assert_eq!(
                dests.total_lens_of_full_buckets(&res),
                slice.len() + num_els_split
            );
            num_els_split += slice.len();
            sched.free_slice(slice);
        }

        dests.complete(&mut res);

        res
    }
}

impl<'a> Into<SplitBucket<'a>> for SplittingBucket<'a> {
    fn into(self) -> SplitBucket<'a> {
        SplitBucket {
            children: Box::new(self.children.map(Bucket::Unsplit)),
        }
    }
}

impl<'a> Scheduler<'a> {
    pub fn new() -> Self {
        // TODO preallocate free_slices here
        Self {
            free_slices: vec![],
            top_level: None,
            phantom: PhantomData,
        }
    }

    pub fn split(
        &mut self,
        input: &'a mut [u64],
        output: &'a mut [u64],
        splitter: &mut dyn Splitter<'a>,
    ) {
        let input_len = input.len();
        assert!(input_len % SLICE_SIZE == 0);

        let slices = input.chunks_exact_mut(SLICE_SIZE);

        let l0 = UnsplitBucket {
            slices: slices.collect(),
        };
        // TODO parametrize splits
        let l0shift = 56;
        let l0 = l0.split(self, splitter, l0shift, 0xff);

        debug_assert_eq!(
            l0.children
                .iter()
                .flat_map(|child| &child.slices)
                .map(|slice| slice.len())
                .sum::<usize>(),
            input_len
        );

        let mut top_level = Some(Bucket::Split(l0.into()));
        eprintln!("Finished L0");
        let mut output_ix = 0;

        // TODO replace this with FixedVec?
        let mut stack = Vec::with_capacity(8);
        let mut bucket_id: u64 = 0;
        if let Some(Bucket::Split(SplitBucket { ref mut children })) = top_level {
            stack.push(children.iter_mut().enumerate())
        } else {
            panic!("Destructuring known value failed");
        };

        while let Some(bucket) = stack.last_mut() {
            let Some((ix, child)) = bucket.next() else {
                // this bucket has been fully split, so we can remove it
                stack.pop(); continue;
            };

            let level = stack.len();

            // we *should* always take this branch, since we only create unsplit buckets and never examine a bucket
            // multiple times
            if let Bucket::Unsplit(ref mut unsplit) = *child {
                // if we don't need this "{ix}", then we can remove the `.enumerate()` from `stack`
                let shift = (8 - level as u8) * 8;
                bucket_id = (bucket_id & !(0xFF << shift)) | ((ix as u64) << shift);
                eprint!("\r{bucket_id:#018x}, Splitting L{level} bucket {ix}");

                match unsplit.slices[..] {
                    [] => {
                        // do nothing!
                        *child = Bucket::Sorted;
                        continue;
                    }
                    [ref slice] => {
                        eprint!("; finishing");
                        splitter
                            .split_small(slice, &mut output[output_ix..output_ix + slice.len()]);
                        output_ix += slice.len();
                        *child = Bucket::Sorted;
                        continue;
                    }
                    _ => (),
                }

                let unsplit_len = unsplit
                    .slices
                    .iter()
                    .map(|slice| slice.len())
                    .sum::<usize>();

                // TODO is there a better way to do this and satisfy the borrow checker?
                // we cannot move `unsplit` out of `*child` since we're matching on a variant, and
                // that would leave `*child` partially constructed. But we're going to replace it anyway!
                let mut this_unsplit = UnsplitBucket::default();
                swap(&mut this_unsplit, unsplit);
                let this_split = this_unsplit.split(self, splitter, shift, 0xFF);

                // dbg!((level, ix));
                debug_assert_eq!(
                    this_split
                        .children
                        .iter()
                        .flat_map(|child| &child.slices)
                        .map(|slice| slice.len())
                        .sum::<usize>(),
                    unsplit_len
                );

                *child = Bucket::Split(this_split.into());
            }

            // we *should* always take this branch, since we just created a split bucket
            if let Bucket::Split(SplitBucket { ref mut children }) = *child {
                if level < MAX_LEVEL_SPLIT as usize {
                    stack.push(children.iter_mut().enumerate())
                }
            }
        }

        eprintln!();
        self.top_level = top_level;
    }

    pub fn get_splits(&mut self) -> Vec<&mut [u64]> {
        let mut top_level = None;
        swap(&mut top_level, &mut self.top_level);

        let mut res = vec![];
        let mut stack = vec![];
        let Some(Bucket::Split(SplitBucket { children })) = top_level else { return res; };

        stack.push(children.into_iter());
        while let Some(bucket) = stack.last_mut() {
            let Some(child) = bucket.next() else {
                // this bucket has been fully split, so we can remove it
                stack.pop(); continue;};
            match child {
                Bucket::Split(SplitBucket { children }) => stack.push(children.into_iter()),
                Bucket::Unsplit(UnsplitBucket { slices }) => {
                    res.extend(slices);
                }
                Bucket::Sorted => {}
            }
        }

        res
    }
}

pub trait Splitter<'a> {
    fn split(
        &mut self,
        input: &[u64],
        shift: u8,
        mask: u64,
        output: &mut ActiveSlices<'a>,
        bucket: &mut SplittingBucket<'a>,
        sched: &mut Scheduler<'a>,
    );

    fn split_small(&mut self, input: &[u64], output: &mut [u64]);
}
