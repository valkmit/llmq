const EMPTY: usize = usize::MAX;

#[derive(Debug)]
pub struct RadixTree<T> {
    nodes: Vec<NodeData<T>>,
    root: usize,
}

#[derive(Debug)]
pub struct NodeData<T> {
    prefix: String,
    first_child: usize,
    next_sibling: usize,
    values: Vec<T>,
}

impl<T: Clone> Default for RadixTree<T> {
    fn default() -> Self {
        RadixTree::new()
    }
}

impl<T: Clone> RadixTree<T> {
    pub fn new() -> Self {
        let root = NodeData {
            prefix: String::new(),
            first_child: EMPTY,
            next_sibling: EMPTY,
            values: Vec::new(),
        };
        
        let mut nodes = Vec::new();
        nodes.push(root);
        
        Self {
            nodes,
            root: 0,
        }
    }

    pub fn insert(&mut self, key: &str, value: T) {
        self.insert_at(self.root, key, value);
    }

    fn insert_at(
        &mut self, 
        node_idx: usize, 
        key: &str, 
        value: T
    ) {
        // if we've consumed the whole key, add value here
        if key.is_empty() {
            self.nodes[node_idx].values.push(value);
            return;
        }

        // check each child for longest common prefix
        let mut current_child = self.nodes[node_idx].first_child;
        while current_child != EMPTY {
            let child = &self.nodes[current_child];
            if let Some(common_len) = get_common_prefix_len(key, &child.prefix) {
                if common_len == 0 {
                    // no common prefix, try next sibling
                    current_child = child.next_sibling;
                    continue;
                }

                if common_len == child.prefix.len() {
                    // child's prefix is fully matched, recurse
                    return self.insert_at(current_child, &key[common_len..], value);
                }

                // partial match, split the existing child
                let split_idx = current_child;
                self.split_node(split_idx, common_len);
                return self.insert_at(split_idx, &key[common_len..], value);
            }
            current_child = child.next_sibling;
        }

        // no matching child found, create new one
        let new_node = NodeData {
            prefix: key.to_string(),
            first_child: EMPTY,
            next_sibling: self.nodes[node_idx].first_child,
            values: vec![value],
        };

        let new_idx = self.nodes.len();
        self.nodes.push(new_node);
        self.nodes[node_idx].first_child = new_idx;
    }

    fn split_node(&mut self, node_idx: usize, split_pos: usize) {
        let original_prefix = self.nodes[node_idx].prefix.clone();
        let original_values = self.nodes[node_idx].values.clone();
        
        // create new child with remaining prefix
        let new_child = NodeData {
            prefix: original_prefix[split_pos..].to_string(),
            first_child: self.nodes[node_idx].first_child.clone(),
            next_sibling: EMPTY,
            values: original_values.to_vec(),
        };

        // update original node
        let new_child_idx = self.nodes.len().clone();
        let _ = &self.nodes.push(new_child);
        
        let node = &mut self.nodes[node_idx];
        node.prefix = original_prefix[..split_pos].to_string();
        node.values = Vec::new();
        node.first_child = new_child_idx;
    }

    pub fn find(&self, key: &str) -> Vec<T> {
        let mut matches = Vec::new();
        self.find_at(self.root, key, &mut matches);
        matches
    }

    fn find_at(
        &self, 
        node_idx: usize, 
        key: &str, 
        matches: &mut Vec<T>
    ) {
        let node = &self.nodes[node_idx];
        
        // if this isn't the root, add its values as they match any longer key
        if node_idx != self.root && !node.prefix.is_empty() {
            matches.extend(node.values.clone());
        }

        // if we've matched the whole key, we're done
        if key.is_empty() {
            return;
        }

        // try to find matching child
        let mut current_child = node.first_child;
        while current_child != EMPTY {
            let child = &self.nodes[current_child];
            if key.starts_with(&child.prefix) {
                return self.find_at(current_child, &key[child.prefix.len()..], matches);
            }
            current_child = child.next_sibling;
        }
    }
}

fn get_common_prefix_len(a: &str, b: &str) -> Option<usize> {
    a.chars()
        .zip(b.chars())
        .take_while(|(a, b)| a == b)
        .count()
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_natural_segments() {
        let mut tree = RadixTree::new();
        tree.insert("quote/btc/usdc", 1);
        tree.insert("quote/btc/usdt", 2);
        tree.insert("quote/eth/usdc", 3);

        assert_eq!(tree.find("quote/btc/usdc"), vec![1]);
        assert_eq!(tree.find("quote/btc/usdt"), vec![2]);
        assert_eq!(tree.find("quote/eth/usdc"), vec![3]);
    }

    #[test]
    fn test_prefix_matching() {
        let mut tree = RadixTree::new();
        tree.insert("quote", 1);
        tree.insert("quote/btc", 2);
        tree.insert("quote/btc/usd", 3);

        assert_eq!(tree.find("quote"), vec![1]);
        assert_eq!(tree.find("quote/btc"), vec![1, 2]);
        assert_eq!(tree.find("quote/eth"), vec![1]);
        assert_eq!(tree.find("quote/btc/usd"), vec![1, 2, 3]);
    }

    #[test]
    fn test_splits() {
        let mut tree = RadixTree::new();
        
        tree.insert("abcdefghijklmnop", 1);
        tree.insert("abcdefghijklmnoz", 2);  

        assert_eq!(tree.find("p"), vec![]);
        assert_eq!(tree.find("z"), vec![]);

        tree.insert("abcdefghijk", 3);
        tree.insert("abcdefgh", 4);
        
        assert_eq!(tree.find("abcdefgh"), vec![4]);
        assert_eq!(tree.find("abcdefghijk"), vec![4, 3]);
        assert_eq!(tree.find("abcdefghijklmnop"), vec![4, 3, 1]);
        assert_eq!(tree.find("abcdefghijklmnoz"), vec![4, 3, 2]);
    }
}