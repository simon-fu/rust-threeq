// refer: https://github.com/http-rs/path-table

use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub struct Mqtree<R> {
    value: Option<R>,
    subs: HashMap<String, Mqtree<R>>,
}

impl<R> Mqtree<R> {
    pub fn new() -> Self {
        Self {
            value: None,
            subs: HashMap::new(),
        }
    }

    pub fn entry(&mut self, path: &str) -> &mut Option<R> {
        let mut tree = self;
        for segment in path.split('/') {
            tree = tree
                .subs
                .entry(segment.to_string())
                .or_insert_with(Mqtree::new);
        }
        &mut tree.value
    }

    pub fn match_with<F>(&self, path: &str, callback: &mut F)
    where
        F: FnMut(&R),
    {
        let paths: Vec<&str> = path.split('/').collect();
        self.match0(&paths, 0, callback);
    }

    // pub fn get<'a>(&'a mut self, vec:&'a mut Vec<&'a mut R>) {
    //     if let Some(r)  = &mut self.value {
    //         vec.push(r);
    //     }
    // }

    // pub fn match_to_vec<'a>(&'a mut self, path: &str, vec: &'a mut Vec<&'a R>)
    // {
    //     self.match_with(path, &|r:&R| {
    //         vec.push(r);
    //     });
    // }

    fn traverse0<F>(&self, callback: &mut F)
    where
        F: FnMut(&R),
    {
        if let Some(r) = &self.value {
            callback(r);
        }

        for item in &self.subs {
            item.1.traverse0(callback);
        }
    }

    fn match0<F>(&self, paths: &Vec<&str>, index: usize, callback: &mut F)
    where
        F: FnMut(&R),
    {
        if index >= paths.len() {
            // matched: self
            if let Some(r) = &self.value {
                callback(r);
            }

            if let Some(tree) = self.subs.get("#") {
                // matched: wildcard remains
                tree.traverse0(callback);
            }

            return;
        }

        let key = paths[index];
        if let Some(tree) = self.subs.get(key) {
            tree.match0(paths, index + 1, callback);
        }

        if let Some(tree) = self.subs.get("+") {
            tree.match0(paths, index + 1, callback);
        }

        if let Some(tree) = self.subs.get("#") {
            // matched: wildcard remains
            tree.traverse0(callback);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Default)]
    struct TestTree {
        tree: Mqtree<String>,
    }

    impl TestTree {
        fn new(filters: &[&str]) -> Self {
            let mut self0 = Self {
                tree: Mqtree::default(),
            };
            for str in filters {
                self0.tree.entry(*str).get_or_insert((*str).to_string());
            }
            self0
        }

        fn match_equal(&self, path: &str, paths: &[&str]) {
            let mut expect_result = std::collections::HashSet::new();
            for str in paths {
                expect_result.insert((*str).to_string());
            }

            let mut actual_result = std::collections::HashSet::new();
            self.tree.match_with(path, &mut |s| {
                actual_result.insert(s.to_string());
            });

            assert_eq!(actual_result, expect_result);
        }
    }

    #[test]
    fn test_mqtree_basic() {
        let list = ["t1", "+", "t1/+", "t1/+/t3", "t1/#", "/t1", "/+", "/#"];
        let tree = TestTree::new(&list);
        tree.match_equal("t1", &["t1", "+", "t1/#"]);
        tree.match_equal("t1/t2", &["t1/+", "t1/#"]);
        tree.match_equal("t1/t2/t3", &["t1/#", "t1/+/t3"]);

        tree.match_equal("t1/", &["t1/+", "t1/#"]);
        tree.match_equal("t1/t2/", &["t1/#"]);
        tree.match_equal("t1/t2/t3/", &["t1/#"]);
        tree.match_equal("/t1", &["/#", "/t1", "/+"]);
        tree.match_equal("/t1/t2", &["/#"]);
        tree.match_equal("/t1/t2/t3", &["/#"]);
    }

    #[test]
    fn test_mqtree_wildcard_all() {
        let list = ["+", "#"];
        let tree = TestTree::new(&list);
        tree.match_equal("t1", &["+", "#"]);
        tree.match_equal("t1/t2", &["#"]);
        tree.match_equal("t1/t2/t3", &["#"]);

        tree.match_equal("t1/", &["#"]);
        tree.match_equal("t1/t2/", &["#"]);
        tree.match_equal("t1/t2/t3/", &["#"]);
        tree.match_equal("/t1", &["#"]);
        tree.match_equal("/t1/t2", &["#"]);
        tree.match_equal("/t1/t2/t3", &["#"]);
    }
}
