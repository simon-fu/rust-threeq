use std::{collections::HashMap, sync::RwLock};

use once_cell::sync::OnceCell;

use crate::clustee;

use super::hub::Hub;

// #[derive(Default)]
pub struct Registry {
    pub hubs: RwLock<HashMap<String, Hub>>,
    pub cluster: clustee::Service,
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
        //  .field("x", &self.x)
         .finish()
    }
}


fn get_mut() -> &'static OnceCell<Registry> {
    static INSTANCE: OnceCell<Registry> = OnceCell::new();
    return &INSTANCE;

}

pub fn set(r: Registry) {
    get_mut().set(r).unwrap();
}

pub fn get() -> &'static Registry {
    return get_mut().get().unwrap();
}




