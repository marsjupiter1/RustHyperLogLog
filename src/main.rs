mod hyper_log_log;
mod random_set;


// how many numbers to place in the bitmap and how many will be different
const SAMPLESIZE: u32=300;
const UNIQUES: u32=100;
// the dataset we create will be keyed into buckets on this
// number of bits, so will be 2^BITS long
// a smaller number of bits will be less accurate
// but more memory efficient
const BITS: u32=7;
// play with the overlap of the datasets
const SET1STARTNUMBER: u32=0;
const SET2STARTNUMBER: u32=20;

fn main()
{
   let  mut log1 = hyper_log_log::init(BITS);

   // these are only randomised in order and in which elements are duplicated
   // we can create small sets and print them to see if this works
   let  set1 = random_set::new(SET1STARTNUMBER, SAMPLESIZE, UNIQUES);
   let  set2 = random_set::new(SET2STARTNUMBER, SAMPLESIZE, UNIQUES);

   for i  in 0..SAMPLESIZE + UNIQUES
   {
      log1.add_datum(set1.get_element(i));
   }
   let  c = log1.estimate_cardinality();

   println!("Sample {} Unique {} Estimated Unique {}", SAMPLESIZE + UNIQUES, UNIQUES, c);

   let  mut log2 = hyper_log_log::init(BITS);
   for i  in 0..SAMPLESIZE + UNIQUES
   {
      log2.add_datum(set2.get_element(i));
   }
 
   let c = log2.estimate_cardinality();

   println!("Sample {} Unique {} Estimated Unique {}", SAMPLESIZE + UNIQUES, UNIQUES, c);

   let  log3 = log1.set_union(log2);

   let c = log3.estimate_cardinality();

   println!("Union Estimated Unique {}", c);

  
}