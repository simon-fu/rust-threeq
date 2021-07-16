use std::time::Instant;

#[derive(Debug)]
pub struct Pacer{
    kick_time : Instant,
    rate : u64,
}

impl Pacer{
    pub fn new(rate : u64) -> Self {
        Pacer {
            kick_time: Instant::now(),
            rate,
        }
    }

    pub fn with_time(mut self, t: Instant) -> Self {
        self.kick_time = t;
        self
    }

    pub fn kick(&mut self) {
        self.kick_time = Instant::now();
        // self.kick_time.elapsed();
    }

    // pub fn elapsed(&self) -> Duration {
    //     self.kick_time.elapsed()
    // }

    pub fn kick_time(&self) -> &Instant {
        &self.kick_time
    }

    pub fn get_wait_milli(&self, n : u64) -> i64{
        if self.rate == 0 {
            return std::i64::MAX/2;
        }
        
        let expect = 1000 * n / self.rate;
        let diff = expect as i64 - self.kick_time.elapsed().as_millis() as i64;
        return diff;
    }

    pub async fn check_wait(&self, n : u64) {
        let diff = self.get_wait_milli(n);
        if diff > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(diff as u64)).await;
        }
    }

    pub async fn run_if_wait<'a, F, Fut>(&self, n : u64, f: F)
    where
        F: Fn() -> Fut,
        Fut: futures::Future<Output = bool>,
    {
        let mut diff = self.get_wait_milli(n);
        let mut is_run_next = true;
        while diff > 0 {
            if is_run_next {
                is_run_next = f().await;
            } else {
                tokio::time::sleep(tokio::time::Duration::from_millis(diff as u64)).await;
            }
            diff = self.get_wait_milli(n);
        }
    }

    // pub async fn run_if_wait2<T>(&self,  n : u64, f: T) 
    // where
    //     T: futures::Future<Output = bool>,
    // {
    //     let mut diff = self.get_wait_milli(n);
    //     let mut is_run_next = true;
    //     while diff > 0 {
    //         if is_run_next {
    //             is_run_next = f.await;
    //         } else {
    //             tokio::time::sleep(tokio::time::Duration::from_millis(diff as u64)).await;
    //         }
    //         diff = self.get_wait_milli(n);
    //     }
    // }
}





// pub async fn run_it< F, Fut, >(n : &u64, f: &F )
// where
//     F: Fn(&u64) -> Fut,
//     Fut: futures::Future<Output = bool>,
// {
//     for i in 0..*n {
//         if f(&i).await {
//             break;
//         }
//     }
// }

// async fn callback(nn: &u64) -> bool {
//     return *nn > 0;
// }

// pub struct Tester{
//     n: u64
// }
// impl<'a> Tester {
//     pub async fn test(&'a mut self, d: std::time::Duration) -> bool{
//         // run_it(&1, callback).await;
//         let block = |nn:&u64| async {
//             return *nn > self.n ;
//         };

//         run_it(&2, &block).await;
//         return self.n > 0 && d.as_millis() > 0;
//     }
// }



#[derive(Debug)]
pub struct Interval{
    next_time : Instant,
    milli: u64,
}

impl Interval{
    pub fn new(milli : u64) -> Self {
        Interval {
            next_time: Instant::now() + std::time::Duration::from_millis(milli),
            milli,
        }
    }

    pub fn check(&mut self) -> bool {
        let now = Instant::now();
        if now >= self.next_time {
            self.next_time = now + std::time::Duration::from_millis(self.milli);
            return true;
        } else{
            return false;
        }
    }
}