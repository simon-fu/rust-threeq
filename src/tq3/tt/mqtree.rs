// refer: https://github.com/http-rs/path-table

use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
struct MqtreeInner<R> {
    value: Option<R>,
    subs: HashMap<String, MqtreeInner<R>>,
}

impl<R> MqtreeInner<R> {
    fn new() -> Self {
        Self {
            value: None,
            subs: HashMap::new(),
        }
    }

    pub fn get(&self, path: &str) -> Option<&R> {
        let mut tree = self;
        for segment in path.split('/') {
            match tree.subs.get(segment) {
                Some(t) => {
                    tree = t;
                }
                None => return None,
            }
        }
        match &tree.value {
            Some(r) => Some(r),
            None => None,
        }
    }

    fn entry(&mut self, path: &str) -> &mut Option<R> {
        let mut tree = self;
        for segment in path.split('/') {
            tree = tree
                .subs
                .entry(segment.to_string())
                .or_insert_with(MqtreeInner::new);
        }
        &mut tree.value
    }

    pub fn remove(&mut self, path: &str) -> bool {
        let paths: Vec<&str> = path.split('/').collect();
        let (exist_value, _empty_subs) = self.remove0(&paths, 0);
        exist_value
    }

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

    fn rmatch0<F>(&self, paths: &Vec<&str>, index: usize, callback: &mut F)
    where
        F: FnMut(&R),
    {
        if index >= paths.len() {
            // matched: self
            if let Some(r) = &self.value {
                callback(r);
            }
            return;
        }

        let key = paths[index];
        if key == "#" {
            self.traverse0(callback);
        } else if key == "+" {
            for r in &self.subs {
                r.1.rmatch0(paths, index + 1, callback);
            }
        } else {
            if let Some(tree) = self.subs.get(key) {
                tree.rmatch0(paths, index + 1, callback);
            }
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

    fn remove0(&mut self, paths: &Vec<&str>, index: usize) -> (bool, bool) {
        if index >= paths.len() {
            return (self.value.is_some(), self.subs.is_empty());
        }

        let key = paths[index];
        if let Some(tree) = self.subs.get_mut(key) {
            let (exist_value, empty_subs) = tree.remove0(paths, index + 1);
            if empty_subs {
                self.subs.remove(key);
            }
            (exist_value, self.subs.is_empty())
        } else {
            (false, false)
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Mqtree<R> {
    inner: MqtreeInner<R>,
}

impl<R> Mqtree<R> {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: MqtreeInner::new(),
        }
    }

    #[inline]
    pub fn entry(&mut self, path: &str) -> &mut Option<R> {
        self.inner.entry(path)
    }

    #[inline]
    pub fn get(&self, path: &str) -> Option<&R> {
        self.inner.get(path)
    }

    #[inline]
    pub fn remove(&mut self, path: &str) -> bool {
        self.inner.remove(path)
    }

    #[inline]
    pub fn match_with<F>(&self, path: &str, callback: &mut F)
    where
        F: FnMut(&R),
    {
        let paths: Vec<&str> = path.split('/').collect();
        self.inner.match0(&paths, 0, callback);
    }
}

#[derive(Clone, Debug, Default)]
pub struct MqtreeR<R> {
    inner: MqtreeInner<R>,
}

impl<R> MqtreeR<R> {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: MqtreeInner::new(),
        }
    }

    #[inline]
    pub fn entry(&mut self, path: &str) -> &mut Option<R> {
        self.inner.entry(path)
    }

    #[inline]
    pub fn get(&self, path: &str) -> Option<&R> {
        self.inner.get(path)
    }

    #[inline]
    pub fn remove(&mut self, path: &str) -> bool {
        self.inner.remove(path)
    }

    #[inline]
    pub fn rmatch_with<F>(&self, path: &str, callback: &mut F)
    where
        F: FnMut(&R),
    {
        let paths: Vec<&str> = path.split('/').collect();
        self.inner.rmatch0(&paths, 0, callback);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Default)]
    struct TestMqTree {
        tree: Mqtree<String>,
    }

    impl TestMqTree {
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

    #[derive(Clone, Debug, Default)]
    struct TestMqTreeR {
        tree: MqtreeR<String>,
    }

    impl TestMqTreeR {
        fn new(paths: &[&str]) -> Self {
            let mut self0 = Self {
                tree: MqtreeR::default(),
            };
            for str in paths {
                self0.tree.entry(*str).get_or_insert((*str).to_string());
            }
            self0
        }

        fn rmatch_equal(&self, path: &str, paths: &[&str]) {
            let mut expect_result = std::collections::HashSet::new();
            for str in paths {
                expect_result.insert((*str).to_string());
            }

            let mut actual_result = std::collections::HashSet::new();
            self.tree.rmatch_with(path, &mut |s| {
                actual_result.insert(s.to_string());
            });

            assert_eq!(actual_result, expect_result);
        }
    }

    #[test]
    fn test_mqtree_match_basic() {
        let list = ["t1", "+", "t1/+", "t1/+/t3", "t1/#", "/t1", "/+", "/#"];
        let tree = TestMqTree::new(&list);
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
    fn test_mqtree_match_wildcard() {
        let list = ["+", "#"];
        let tree = TestMqTree::new(&list);
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

    #[test]
    fn test_mqtree_rmatch_1() {
        let list = ["t1", "t1/t2", "t1/t2/t3"];
        let tree = TestMqTreeR::new(&list);

        tree.rmatch_equal("t1", &["t1"]);
        tree.rmatch_equal("t1/t2", &["t1/t2"]);
        tree.rmatch_equal("t1/t2/t3", &["t1/t2/t3"]);

        tree.rmatch_equal("t1/", &[]);
        tree.rmatch_equal("t1/t2/", &[]);
        tree.rmatch_equal("t1/t2/t3/", &[]);

        tree.rmatch_equal("/t1", &[]);
        tree.rmatch_equal("/t1/t2", &[]);
        tree.rmatch_equal("/t1/t2/t3", &[]);

        tree.rmatch_equal("#", &list);
        tree.rmatch_equal("t1/#", &list);
        tree.rmatch_equal("t1/t2/#", &["t1/t2", "t1/t2/t3"]);

        tree.rmatch_equal("+", &["t1"]);
        tree.rmatch_equal("+/", &[]);

        tree.rmatch_equal("+/+", &["t1/t2"]);
        tree.rmatch_equal("+/+/", &[]);

        tree.rmatch_equal("+/+/+", &["t1/t2/t3"]);
        tree.rmatch_equal("+/+/+/", &[]);
        tree.rmatch_equal("+/+/+/+", &[]);

        tree.rmatch_equal("+/+/+/+/", &[]);
        tree.rmatch_equal("+/+/+/+/+", &[]);

        tree.rmatch_equal("t1/+", &["t1/t2"]);
        tree.rmatch_equal("t1/+/", &[]);
        tree.rmatch_equal("+/t2", &["t1/t2"]);
        tree.rmatch_equal("+/t2/", &[]);

        tree.rmatch_equal("+/t2/t3", &["t1/t2/t3"]);
        tree.rmatch_equal("+/t2/t3/", &[]);
        tree.rmatch_equal("t1/+/t3", &["t1/t2/t3"]);
        tree.rmatch_equal("t1/+/t3/", &[]);
        tree.rmatch_equal("t1/t2/+", &["t1/t2/t3"]);
        tree.rmatch_equal("t1/t2/+/", &[]);
        tree.rmatch_equal("t1/t2/t3/+", &[]);
        tree.rmatch_equal("t1/t2/t3/+/", &[]);

        //tree.rmatch_equal(aaa, &[aaa]);
    }

    #[test]
    fn test_mqtree_rmatch_2() {
        let list = ["t1/", "t1/t2/", "t1/t2/t3/"];
        let tree = TestMqTreeR::new(&list);

        tree.rmatch_equal("t1", &[]);
        tree.rmatch_equal("t1/t2", &[]);
        tree.rmatch_equal("t1/t2/t3", &[]);

        tree.rmatch_equal("t1/", &["t1/"]);
        tree.rmatch_equal("t1/t2/", &["t1/t2/"]);
        tree.rmatch_equal("t1/t2/t3/", &["t1/t2/t3/"]);

        tree.rmatch_equal("/t1", &[]);
        tree.rmatch_equal("/t1/t2", &[]);
        tree.rmatch_equal("/t1/t2/t3", &[]);

        tree.rmatch_equal("#", &list);
        tree.rmatch_equal("t1/#", &list);
        tree.rmatch_equal("t1/t2/#", &["t1/t2/", "t1/t2/t3/"]);

        tree.rmatch_equal("+", &[]);
        tree.rmatch_equal("+/", &["t1/"]);

        tree.rmatch_equal("+/+", &["t1/"]);
        tree.rmatch_equal("+/+/", &["t1/t2/"]);

        tree.rmatch_equal("+/+/+", &["t1/t2/"]);
        tree.rmatch_equal("+/+/+/", &["t1/t2/t3/"]);
        tree.rmatch_equal("+/+/+/+", &["t1/t2/t3/"]);

        tree.rmatch_equal("+/+/+/+/", &[]);
        tree.rmatch_equal("+/+/+/+/+", &[]);

        tree.rmatch_equal("t1/+", &["t1/"]);
        tree.rmatch_equal("t1/+/", &["t1/t2/"]);
        tree.rmatch_equal("+/t2", &[]);
        tree.rmatch_equal("+/t2/", &["t1/t2/"]);

        tree.rmatch_equal("+/t2/t3", &[]);
        tree.rmatch_equal("+/t2/t3/", &["t1/t2/t3/"]);
        tree.rmatch_equal("t1/+/t3", &[]);
        tree.rmatch_equal("t1/+/t3/", &["t1/t2/t3/"]);
        tree.rmatch_equal("t1/t2/+", &["t1/t2/"]);
        tree.rmatch_equal("t1/t2/+/", &["t1/t2/t3/"]);
        tree.rmatch_equal("t1/t2/t3/+", &["t1/t2/t3/"]);
        tree.rmatch_equal("t1/t2/t3/+/", &[]);
    }
}
