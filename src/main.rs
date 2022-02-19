use clap::{Parser, Subcommand};
use eyre::Result;
use rayon::prelude::*;
use serde::Serialize;
use serde_json::Value;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fmt;
use std::fmt::Display;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use walkdir::WalkDir;

mod query;

#[derive(PartialEq, Eq, Hash, Debug, Serialize)]
struct KeyString(String);

impl KeyString {
    fn new(key: &str) -> Self {
        KeyString(key.to_string())
    }
}

impl Display for KeyString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.contains('.') {
            write!(f, "\"{}\"", self.0)
        } else {
            write!(f, "{}", self.0)
        }
    }
}

type KMFiles = HashSet<Arc<PathBuf>>;

#[derive(Debug, Serialize)]
struct KMNull {
    files: KMFiles,
}

impl KMNull {
    fn new() -> Self {
        Self {
            files: HashSet::new(),
        }
    }

    fn merge(&mut self, other: Self) {
        self.files.extend(other.files);
    }
    fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
    fn count(&self) -> usize {
        self.files.len()
    }
    fn files(&self) -> Box<dyn Iterator<Item = &Arc<PathBuf>> + '_> {
        Box::new(self.files.iter())
    }
}
#[derive(Debug, Serialize)]
struct KMBool {
    t: KMFiles,
    f: KMFiles,
}
impl KMBool {
    fn new() -> Self {
        Self {
            t: HashSet::new(),
            f: HashSet::new(),
        }
    }
    fn merge(&mut self, other: Self) {
        self.f.extend(other.f);
        self.t.extend(other.t);
    }
    fn is_empty(&self) -> bool {
        self.t.is_empty() && self.f.is_empty()
    }
    fn count(&self) -> usize {
        self.t.len() + self.f.len()
    }
    fn files(&self) -> Box<dyn Iterator<Item = &Arc<PathBuf>> + '_> {
        Box::new(self.t.iter().chain(self.f.iter()))
    }
}
#[derive(Debug, Serialize)]
struct KMNumber {
    files: KMFiles,
    int: HashSet<i64>,
    float: HashSet<u64>,
}
impl KMNumber {
    fn new() -> Self {
        Self {
            files: HashSet::new(),
            int: HashSet::new(),
            float: HashSet::new(),
        }
    }
    fn merge(&mut self, other: Self) {
        self.files.extend(other.files);
        self.int.extend(other.int);
        self.float.extend(other.float);
    }
    fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
    fn count(&self) -> usize {
        self.files.len()
    }
    fn files(&self) -> Box<dyn Iterator<Item = &Arc<PathBuf>> + '_> {
        Box::new(self.files.iter())
    }
}
#[derive(Debug, Serialize)]
struct KMString {
    files: KMFiles,
}
impl KMString {
    fn new() -> Self {
        Self {
            files: HashSet::new(),
        }
    }
    fn merge(&mut self, other: Self) {
        self.files.extend(other.files);
    }
    fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
    fn count(&self) -> usize {
        self.files.len()
    }
    fn files(&self) -> Box<dyn Iterator<Item = &Arc<PathBuf>> + '_> {
        Box::new(self.files.iter())
    }
}
#[derive(Debug, Serialize)]
struct KMArray {
    items: Option<Box<KMTypes>>,
    min_len: usize,
    max_len: usize,
}
impl KMArray {
    fn new() -> Self {
        Self {
            min_len: 0,
            max_len: 0,
            items: None,
        }
    }
    fn _get_items(&mut self) -> &mut Box<KMTypes> {
        self.items.get_or_insert(Box::new(KMTypes::new()))
    }
    fn merge(&mut self, other: Self) {
        self.min_len = std::cmp::min(self.min_len, other.min_len);
        self.max_len = std::cmp::max(self.max_len, other.max_len);
        if let Some(items) = other.items {
            self._get_items().merge(*items);
        }
    }
    fn add(&mut self, file: Arc<PathBuf>, json_value: &serde_json::Value) {
        self._get_items().add(file, json_value);
    }
    fn is_empty(&self) -> bool {
        self.items.is_none() || self.items.as_ref().unwrap().is_empty()
    }
    fn count(&self) -> usize {
        if let Some(items) = &self.items {
            items.count()
        } else {
            0
        }
    }
    fn files(&self) -> Box<dyn Iterator<Item = &Arc<PathBuf>> + '_> {
        if let Some(items) = &self.items {
            items.files()
        } else {
            Box::new(std::iter::empty())
        }
    }
}
impl Display for KMArray {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.count();
        write!(f, "[")?;
        if let Some(items) = &self.items {
            write!(f, "{items} ")?;
        }
        write!(f, "]={count} ")
    }
}

#[derive(Debug, Serialize)]
struct KMObject {
    files: KMFiles,
}
impl KMObject {
    fn new() -> Self {
        Self {
            files: HashSet::new(),
        }
    }
    fn merge(&mut self, other: Self) {
        self.files.extend(other.files);
    }
    fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
    fn count(&self) -> usize {
        self.files.len()
    }
    fn files(&self) -> Box<dyn Iterator<Item = &Arc<PathBuf>> + '_> {
        Box::new(self.files.iter())
    }
}

#[derive(Debug, Serialize)]
struct KMTypes {
    #[serde(skip_serializing_if = "KMNull::is_empty")]
    null: KMNull,
    #[serde(skip_serializing_if = "KMBool::is_empty")]
    bool: KMBool,
    #[serde(skip_serializing_if = "KMString::is_empty")]
    string: KMString,
    #[serde(skip_serializing_if = "KMNumber::is_empty")]
    number: KMNumber,
    #[serde(skip_serializing_if = "KMArray::is_empty")]
    array: KMArray,
    #[serde(skip_serializing_if = "KMObject::is_empty")]
    object: KMObject,
}

impl KMTypes {
    fn new() -> Self {
        Self {
            null: KMNull::new(),
            bool: KMBool::new(),
            string: KMString::new(),
            number: KMNumber::new(),
            array: KMArray::new(),
            object: KMObject::new(),
        }
    }
    fn add(&mut self, file: Arc<PathBuf>, json_value: &serde_json::Value) {
        match json_value {
            Value::Null => {
                self.null.files.insert(file);
            }
            Value::Bool(value) => {
                if *value {
                    self.bool.t.insert(file);
                } else {
                    self.bool.t.insert(file);
                }
            }
            Value::Number(n) => {
                self.number.files.insert(file);
                if n.is_i64() {
                    self.number.int.insert(n.as_i64().unwrap());
                } else if n.is_f64() {
                    self.number.float.insert(n.as_f64().unwrap().to_bits());
                }
            }
            Value::String(_) => {
                self.string.files.insert(file);
            }
            Value::Array(array) => {
                let len = array.len();
                if len < self.array.min_len {
                    self.array.min_len = len;
                }
                if len > self.array.max_len {
                    self.array.max_len = len;
                }
                for item in array {
                    let clone_path = file.clone();
                    self.array.add(clone_path, item);
                }
            }
            Value::Object(_map) => {
                self.object.files.insert(file);
            }
        }
    }

    fn merge(&mut self, other: Self) {
        self.null.merge(other.null);
        self.bool.merge(other.bool);
        self.number.merge(other.number);
        self.string.merge(other.string);
        self.array.merge(other.array);
        self.object.merge(other.object);
    }
    fn is_empty(&self) -> bool {
        self.null.is_empty()
            && self.bool.is_empty()
            && self.string.is_empty()
            && self.number.is_empty()
            && self.array.is_empty()
            && self.object.is_empty()
    }
    fn is_object(&self) -> bool {
        self.null.is_empty()
            && self.bool.is_empty()
            && self.string.is_empty()
            && self.number.is_empty()
            && self.array.is_empty()
    }
    fn type_count(&self) -> u8 {
        !self.null.is_empty() as u8
            + !self.bool.is_empty() as u8
            + !self.string.is_empty() as u8
            + !self.number.is_empty() as u8
            + !self.array.is_empty() as u8
    }
    fn count(&self) -> usize {
        self.null.count()
            + self.bool.count()
            + self.string.count()
            + self.number.count()
            + self.array.count()
            + self.object.count()
    }

    fn files(&self) -> Box<dyn Iterator<Item = &Arc<PathBuf>> + '_> {
        let nulls = self.null.files();
        let bool = self.bool.files();
        let number = self.number.files();
        let string = self.string.files();
        let array = self.array.files();
        let object = self.object.files();
        Box::new(
            nulls
                .chain(bool)
                .chain(number)
                .chain(string)
                .chain(array)
                .chain(object),
        )
    }
}
impl Display for KMTypes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.null.files.is_empty() {
            let count = self.null.count();
            write!(f, "N={count} ")?;
        }
        if !(self.bool.f.is_empty() && self.bool.f.is_empty()) {
            let count = self.bool.count();
            write!(f, "B={count} ")?;
        }
        if !self.number.files.is_empty() {
            let count = self.number.count();
            write!(f, "Num={count} ")?;
        }
        if !self.string.files.is_empty() {
            let count = self.string.count();
            write!(f, "Str={count} ")?;
        }
        if !self.array.is_empty() {
            write!(f, "{} ", self.array)?;
        }
        if !self.object.files.is_empty() {
            let count = self.object.count();
            write!(f, "{{}}={count}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct KeyMap {
    count: u64,
    types: KMTypes,
    keys: HashMap<KeyString, KeyMap>,
}
impl KeyMap {
    fn new() -> Self {
        let keys = HashMap::new();
        let types = KMTypes::new();
        Self {
            keys,
            types,
            count: 0,
        }
    }

    fn add(&mut self, file: &Arc<PathBuf>, name: &str, value: &serde_json::Value) {
        let sub_tree = self
            .keys
            .entry(KeyString::new(name))
            .or_insert_with(KeyMap::new);
        sub_tree.count += 1;
        if let Value::Object(v_map) = value {
            for (k, v) in v_map {
                sub_tree.add(file, k, v);
            }
        }
        let file = file.clone();
        sub_tree.types.add(file, value);
    }

    fn merge(&mut self, other: Self) {
        self.types.merge(other.types);
        for (key, key_map) in other.keys {
            let entry = self.keys.entry(key);
            match entry {
                Entry::Occupied(_) => {
                    entry.and_modify(|e| e.merge(key_map));
                }
                Entry::Vacant(_) => {
                    entry.or_insert(key_map);
                }
            }
        }
        self.count += other.count;
    }
}

fn read_json_file<P: AsRef<Path>>(path: P) -> Result<Value> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let value = serde_json::from_reader(reader)?;

    Ok(value)
}

fn print_sub_keys<'tree>(tree: &'tree KeyMap, type_count:u8, prefix: &mut Vec<&'tree KeyString>) {
    let count = tree.count;
    let types = &tree.types;
    if !types.is_object() && types.type_count() >= type_count{
        print!("{count} '");
        for p in prefix.iter() {
            print!(".{p}");
        }
        println!("' {types}");
    }
    for (k, v) in &tree.keys {
        prefix.push(k);
        print_sub_keys(v, type_count, prefix);
        prefix.pop();
    }
}

fn print_keys(tree: &KeyMap, type_count:u8) {
    let mut prefix: Vec<&KeyString> = vec![];
    print_sub_keys(&tree, type_count, &mut prefix)
}

fn print_query(tree: &KeyMap, q: &str) -> Result<()> {
    let (_rest, keys) =
        query::query(q).map_err(|e| eyre::eyre!("Failed to parse query:\n\t{}", e))?;
    let mut tree = tree;
    for k in keys {
        match tree.keys.get(&KeyString::new(k)) {
            None => {
                return Err(eyre::eyre!("Could not resolve key {}", k));
            }
            Some(sub_tree) => {
                tree = sub_tree;
            }
        }
    }
    //for f in tree.types.files() {
    //        println!("{}", f.strip_prefix(&args.dir)?.to_string_lossy());
    //}
    let json_str = serde_json::to_string(tree)?;
    println!("{json_str}");
    Ok(())
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    dir: PathBuf,
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Query the analytics of a specific member 
    Query {
        /// the query is similar to a jq query ".a.b.c"
        query: String
    },
    /// List all member keys with types and how often this member is in the dataset
    Keys {
        /// filter all member which have at lest [TYPE_COUNT] types
        #[clap(long, default_value_t = 1)]
        type_count: u8 
    },
}

fn main() -> Result<()> {
    let args = Args::parse();
    let ext = Some(OsStr::new("json"));
    let files: Vec<_> = WalkDir::new(&args.dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file() && e.path().extension() == ext)
        .map(|entry| Arc::new(entry.into_path()))
        .collect();
    let tree = files
        .par_iter()
        .filter_map(|file| match read_json_file(&**file) {
            Err(_) => None,
            Ok(json) => {
                let mut sub_map = KeyMap::new();
                if let Value::Object(m) = json {
                    for (k, v) in m {
                        sub_map.add(file, &k, &v);
                    }
                }
                Some(sub_map)
            }
        })
        .reduce(KeyMap::new, |mut a, b| {
            a.merge(b);
            a
        });
    match &args.cmd {
        Command::Query { query } => {
            print_query(&tree, &query)?;
        }
        Command::Keys{type_count} => {
            print_keys(&tree, *type_count);
        }
    }
    Ok(())
}
