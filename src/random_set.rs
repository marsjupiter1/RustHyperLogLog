
use rand::Rng;
pub struct Set {
    element_set: Vec<u32>,
}

impl Set {
    pub fn get_element(&self, i: u32) -> u32 {
        self.element_set[i as usize]
    }

    fn get_random(low: u32, high: u32) -> u32 {
        rand::thread_rng().gen_range(low..high) as u32
    }
}

pub fn new(start_number: u32, num_elements: u32, num_unique_elements: u32) -> Set {
    let mut v = Vec::with_capacity((num_elements * 2 - num_unique_elements) as usize);
    v.resize((num_elements * 2 - num_unique_elements) as usize, 0);
    let mut set = Set {
       element_set: v,
    };

    // Create the set of unique elements to use
    for i in start_number as usize..num_unique_elements as usize + start_number as usize{
        set.element_set[i - start_number as usize] = i as u32;
    }

    // Create duplicates
    for i in 0..num_elements as usize - num_unique_elements as usize {
        set.element_set[i + num_unique_elements as usize] =
        set.element_set[Set::get_random(0, num_unique_elements - 1) as usize];
    }
    // shuffle
    for i in 0..num_elements as usize + num_unique_elements as usize {
        let swappos = Set::get_random(0, num_elements + num_unique_elements - 1) as usize;
        let swap = set.element_set[i];
        set.element_set[i] = set.element_set[swappos];
        set.element_set[swappos] = swap;
    }
    set
}
