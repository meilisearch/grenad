use std::convert::TryInto;
use std::ops::Deref;

/// A slice wrapper that contains a slice with at least `N` elements.
///
/// ```
/// use grenad::SliceAtLeast;
///
/// let hello = &[1, 2, 3, 4, 5];
/// let nslice = SliceAtLeast::<i32, 2>::new(hello).unwrap();
///
/// let ([first, second], tail) = nslice.deconstruct_front();
/// assert_eq!(*first, 1);
/// assert_eq!(*second, 2);
/// assert_eq!(tail, &[3, 4, 5]);
///
/// let (head, [before_last, last]) = nslice.deconstruct_end();
/// assert_eq!(*before_last, 4);
/// assert_eq!(*last, 5);
/// assert_eq!(head, &[1, 2, 3]);
/// ```
#[repr(transparent)]
pub struct SliceAtLeast<T, const N: usize>([T]);

impl<T, const N: usize> SliceAtLeast<T, N> {
    /// Construct a slice wrapper with at least `N` elements in it.
    #[inline]
    pub fn new(slice: &[T]) -> Option<&SliceAtLeast<T, N>> {
        if slice.len() >= N {
            Some(unsafe { &*(slice as *const [T] as *const _) })
        } else {
            None
        }
    }

    /// Returns the minimum number of elements this wrapper contains.
    #[inline]
    pub const fn at_least(&self) -> usize {
        N
    }

    /// Deconstruct the internal slice into an array of at least `N` elements
    /// from the front of the slice and the rest of the slice.
    #[inline]
    pub fn deconstruct_front(&self) -> (&[T; N], &[T]) {
        let (head, tail) = self.0.split_at(N);
        let head = head.try_into().unwrap();
        (head, tail)
    }

    /// Deconstruct the internal slice into an array of at least `N` elements
    /// from the end of the slice and the rest of the slice.
    #[inline]
    pub fn deconstruct_end(&self) -> (&[T], &[T; N]) {
        let (head, tail) = self.0.split_at(self.len() - N);
        let tail = tail.try_into().unwrap();
        (head, tail)
    }
}

impl<T, const N: usize> AsRef<[T]> for SliceAtLeast<T, N> {
    fn as_ref(&self) -> &[T] {
        &self.0
    }
}

impl<T, const N: usize> Deref for SliceAtLeast<T, N> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
