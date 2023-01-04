use std::ops::Deref;

#[macro_export]
macro_rules! define_immutable_id {
    ($id1:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Copy, Hash, Default)]
        #[derive(Serialize, Deserialize)]
        pub struct $id1(u64);
        
        impl $id1 {
            #[inline]
            pub fn new(id: u64) -> Self {
                Self(id)
            }

            #[inline]
            pub fn to(&self) -> u64 {
                self.0
            }
        }

        impl Deref for $id1 {
            type Target = u64;
            #[inline]
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl From<$id1> for u64 {
            #[inline]
            fn from(id: $id1) -> Self {
                id.to()
                // *id.deref()
            }
        }
        
        impl From<u64> for $id1 {
            #[inline]
            fn from(id: u64) -> Self {
                $id1(id)
            }
        }

        impl std::ops::Add<u64> for $id1 {
            type Output = Self;
        
            #[inline]
            fn add(self, rhs: u64) -> Self::Output {
                Self(self.0 + rhs)
            }
        }
        
        impl std::ops::Sub for $id1 {
            type Output = u64;
        
            #[inline]
            fn sub(self, rhs: Self) -> Self::Output {
                self.0 - rhs.0
            }
        }

        impl std::fmt::Display for $id1 {
            #[inline]
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    }
}

#[macro_export]
macro_rules! define_mutable_id {
    ($id1:ident) => {
        
        define_immutable_id!($id1);
        
        impl $id1 {        
            #[inline]
            pub fn next(&mut self) {
                self.0 += 1;
            }
        
            #[inline]
            pub fn add(&mut self, n: u64) {
                self.0 += n;
            }
        
            #[inline]
            pub fn sub(&mut self, n: u64) {
                self.0 -= n;
            }
        }
        
        impl DerefMut for $id1 {
            #[inline]
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

    }
}

// pub(crate) trait To<T: Sized> {
//     fn to(self) -> T;
// }

// #[macro_export]
// macro_rules! define_immuable_to_id {
//     ($id1:ident) => {
//         define_immutable_id!($id1);

//         impl To<u64> for $id1 {
//             #[inline]
//             fn to(self) -> u64 {
//                 self.0
//             }
//         }

//         impl To<Option<u64>> for Option<$id1> {
//             #[inline]
//             fn to(self) -> Option<u64> {
//                 self.map(|n| n.0)
//                 // match self {
//                 //     Some(id) => Some(id.0),
//                 //     None => None,
//                 // }
//             }
//         }

//         impl To<Option<$id1>> for Option<u64> {
//             #[inline]
//             fn to(self) -> Option<$id1> {
//                 self.map(|n| $id1(n))
//             }
//         }
//     }
// }

define_immutable_id!(NodeId);

define_immutable_id!(ChId);

define_immutable_id!(NodeOffset);

/// 生成本地内存里唯一
pub fn next_instance_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};

    lazy_static::lazy_static! {
        static ref INST_ID: AtomicU64 = AtomicU64::new(1);
    };
    let inst_id = INST_ID.fetch_add(1, Ordering::Relaxed);
    inst_id
}
