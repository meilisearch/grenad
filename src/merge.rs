/// This trait defines the way multiple values must be merged.
pub trait Merge {
    /// The error that can be raised when merging the values.
    type Error;
    /// The type that is returned by the merge method.
    type Output: AsRef<[u8]>;

    /// The function that merges the values that correspond to the same key.
    fn merge<I, A>(&self, key: &[u8], values: I) -> Result<Self::Output, Self::Error>
    where
        I: IntoIterator<Item = A>,
        A: AsRef<[u8]>;
}

impl<M: Merge> Merge for &M {
    type Error = <M as Merge>::Error;
    type Output = <M as Merge>::Output;

    fn merge<I, A>(&self, key: &[u8], values: I) -> Result<Self::Output, Self::Error>
    where
        I: IntoIterator<Item = A>,
        A: AsRef<[u8]>,
    {
        (**self).merge(key, values)
    }
}
