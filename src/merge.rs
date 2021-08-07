pub trait Merge {
    type Error;
    type Output: AsRef<[u8]>;

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
