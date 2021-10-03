use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use once_cell::sync::OnceCell;

// use crate::discovery;

use super::hub::Hub;

#[derive(Debug)]
pub struct Tenant {
    pub uid: String,
    pub hub: Hub,
}

// #[derive(Default)]
pub struct Registry {
    pub tenants: RwLock<HashMap<String, Arc<Tenant>>>,
    // pub discovery: discovery::Service,
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
