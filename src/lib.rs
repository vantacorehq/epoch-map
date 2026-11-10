#[inline]
fn hash(&self, key: &K) -> usize {
    // Simple deterministic hash that works in no_std
    let mut h: u64 = 0xcbf29ce484222325;
    let bytes = unsafe {
        core::slice::from_raw_parts(
            key as *const K as *const u8,
            core::mem::size_of::<K>(),
        )
    };
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    (h as usize) & (N - 1)
}


        
       

   

  
               
                      
                       
