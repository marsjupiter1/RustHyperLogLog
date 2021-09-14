#![allow(dead_code)]
use std::cmp;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
//use fasthash::{murmur3, Murmur3Hasher};

const ALPHAS: &'static[f64] =
&[0.0, 0.351193943305104, 0.532434616688025, 0.625608716971165,
	0.673102032011193, 0.697122649688705, 0.709208485323602,
	0.715271255627600, 0.718307770416137, 0.719827413209098,
	0.720587757723026, 0.720968410691135, 0.721159556732532,
	0.721256733328830, 0.721308519914072, 0.721340807633915,
	0.721369740077220

];
//fn hash<T: Hash>(t: &T) -> u64 {
//    let mut s: Murmur3Hasher = Default::default();
//   t.hash(&mut s);
//   s.finish()
//}

#[derive(Debug)]
pub struct Log {
    key_bit_count: u32,
    key_array_size: u32,
    max_zeros: Vec<u8>,
    alpha:f64 
}

impl Log {
    // from the hash take some bits to make a bucket index
    fn inthash_most_significant_bits(datum_hash: u64, n_bits: u32) -> u32 {
        assert!(n_bits <= 32);
        (datum_hash & ((2 as u32).pow(n_bits)-1) as u64) as u32
    }

    // get the leading 0's
    fn inthash_leading_zeros(datum_hash: u64) -> u8 {
        let count = datum_hash.leading_zeros() as u8;
        count
    }
    pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }
    // We can overload this according to data type
    pub fn add_datum(&mut self, datum: u32) {
        let hash = Log::calculate_hash(&datum);

        let index = Log::inthash_most_significant_bits(hash, self.key_bit_count) as usize;
        self.max_zeros[index] =
            cmp::max(self.max_zeros[index], Log::inthash_leading_zeros(hash) + 1);
    }

    pub fn estimate_cardinality(&self) -> f64 {
        let mut total_zeros = 0;
        let mut z: u8;
        let mut sum: f64 =0.0;
        for i in 0..self.key_array_size as usize {
            z = self.max_zeros[i];
           // print!(" {} ",z);
            sum += (2.0 as f64).powf( -(z as f64));
            if z == 0 {
                total_zeros += 1;
            }
        }
        //println!("zeros {} key array size {} sum {}\n",total_zeros,self.key_array_size,sum);

        if total_zeros == 0 {
            return self.alpha * (self.key_array_size.pow(2) as f64) / sum;
        }

        self.key_array_size as f64 * ((self.key_array_size as f64) / (total_zeros as f64)).ln()
    }
    pub fn union(&self, bitmap_ptr: Log) {
        assert!(bitmap_ptr.key_bit_count == self.key_bit_count);
        self.add(bitmap_ptr);
    }
    fn add(&self, mut to_ptr: Log) {
        for i in 0..self.key_array_size as usize {
            to_ptr.max_zeros[i] = cmp::max(to_ptr.max_zeros[i], self.max_zeros[i]);
        }
    }
    pub fn copy(&self) -> Log {
        let mut to = init(self.key_bit_count);
        for i in 0..self.key_array_size as usize {
            to.max_zeros[i] = self.max_zeros[i];
        }
        to
    }

    pub fn set_union(&self, datum: Log) -> Log {
        assert!(datum.key_bit_count == self.key_bit_count);
        let mut ret_val: Log = init(self.key_bit_count);
        ret_val.key_bit_count = self.key_bit_count;
        ret_val.key_array_size = self.key_array_size;

        for i in 0..self.key_array_size as usize {
            ret_val.max_zeros[i] = cmp::max(ret_val.max_zeros[i], self.max_zeros[i]);
        }
        ret_val
    }
    pub fn magnitude_intersection(&self, datum: Log) -> f64 {
        let a = self.estimate_cardinality();
        let b = datum.estimate_cardinality();

        let setunion = self.set_union(datum);
        let aub = setunion.estimate_cardinality();
        a + b - aub
    }
}

pub fn init(keybitcount: u32) -> Log {
    let key_size: u32 = (2 as u32).pow(keybitcount);
    let mut v = Vec::with_capacity(key_size as usize);
    let alpha:f64;
    if keybitcount > 16
	{
		alpha = 0.72136974007722; // It doesn't get much more accurate with higher p
		
	}else{
        alpha = ALPHAS[keybitcount as usize];
    } 
    v.resize(key_size as usize, 0);
    let bitmap = Log {
        key_bit_count: keybitcount,
        key_array_size: key_size,
        max_zeros: v,
        alpha: alpha,
    };

    assert!(keybitcount >= 1);

    bitmap
}
