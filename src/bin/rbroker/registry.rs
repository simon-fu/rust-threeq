// use std::{collections::HashMap, sync::RwLock};

// use super::hub::Hub;

// #[derive(Default)]
// pub struct Registry {
//     pub hubs: RwLock<HashMap<String, Hub>>,
// }

// pub fn get() -> &'static Registry {
//     lazy_static::lazy_static! {
//         static ref INST: Registry = Registry::default();
//     }
//     return &*INST;
// }
