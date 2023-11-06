use std::{
    collections::BTreeMap,
    ptr::NonNull,
    sync::{RwLock, RwLockWriteGuard},
};

pub struct SharedAppendMap<K, V: ?Sized> {
    /// SAFETY: Pointers in this map should never be dropped without a mutable reference to the map.
    inner: RwLock<BTreeMap<K, NonNull<V>>>,
}

pub enum Entry<'a, K, V: ?Sized> {
    Occupied(&'a V),
    Vacant(VacantEntry<'a, K, V>),
}

pub struct VacantEntry<'a, K, V: ?Sized> {
    inner: RwLockWriteGuard<'a, BTreeMap<K, NonNull<V>>>,
    key: K,
}

impl<K, V: ?Sized> SharedAppendMap<K, V> {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn entry(&self, key: K) -> Entry<K, V>
    where
        K: Ord,
    {
        let inner = self.inner.write().unwrap();

        if let Some(ptr) = inner.get(&key) {
            Entry::Occupied(unsafe {
                // SAFETY: Pointer valid until the map is borrowed mutably.
                ptr.as_ref()
            })
        } else {
            Entry::Vacant(VacantEntry { inner, key })
        }
    }

    pub fn insert_or_replace(&mut self, key: K, value: impl Into<Box<V>>) -> Option<Box<V>>
    where
        K: Ord,
    {
        let value = value.into();

        let ptr = NonNull::from(Box::leak(value));
        let old = self.inner.get_mut().unwrap().insert(key, ptr);

        old.map(|old| unsafe {
            // SAFETY: Pointer valid.
            Box::from_raw(old.as_ptr())
        })
    }
}

impl<'a, K, V: ?Sized> VacantEntry<'a, K, V> {
    pub fn insert(mut self, value: impl Into<Box<V>>) -> &'a V
    where
        K: Ord,
    {
        let value = value.into();

        let ptr = NonNull::from(Box::leak(value));
        let old = self.inner.insert(self.key, ptr);

        // A VacantEntry should only be created if the key is not present in the map.
        // If this were not true then we would have a memory leak, as we are not allowed to drop the
        // old ptr because we don't have a mutable reference to the map.
        debug_assert!(old.is_none());

        unsafe {
            // SAFETY: Pointer valid until the map is borrowed mutably.
            ptr.as_ref()
        }
    }
}

impl<K, V: ?Sized> Drop for SharedAppendMap<K, V> {
    fn drop(&mut self) {
        let inner = self.inner.get_mut().unwrap();

        for ptr in inner.values() {
            unsafe {
                // SAFETY: Pointer valid.
                drop(Box::from_raw(ptr.as_ptr()));
            }
        }
    }
}
