// Values taken from Donald Knuth's MMIX
const A: u64 = 6364136223846793005;
const C: u64 = 1442695040888963407;

pub struct LCG {
    x: u64,
}

impl LCG {
    pub const fn new() -> Self {
        Self { x: A }
    }

    pub fn next(&mut self) -> u64 {
        let ret = self.x;
        self.x = ret.wrapping_mul(A).wrapping_add(C);
        ret
    }

    pub const fn next_owning(self) -> (u64, Self) {
        let ret = self.x;
        let x = ret.wrapping_mul(A).wrapping_add(C);
        (ret, Self { x })
    }
}
