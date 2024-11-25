use std::borrow::Cow;
use std::result::Result;

use either::Either;

/// A trait defining the way we merge multiple
/// values sharing the same key.
pub trait MergeFunction {
    type Error;
    fn merge<'a>(&self, key: &[u8], values: &[Cow<'a, [u8]>])
        -> Result<Cow<'a, [u8]>, Self::Error>;
}

impl<MF> MergeFunction for &MF
where
    MF: MergeFunction,
{
    type Error = MF::Error;

    fn merge<'a>(
        &self,
        key: &[u8],
        values: &[Cow<'a, [u8]>],
    ) -> Result<Cow<'a, [u8]>, Self::Error> {
        (*self).merge(key, values)
    }
}

impl<MFA, MFB> MergeFunction for Either<MFA, MFB>
where
    MFA: MergeFunction,
    MFB: MergeFunction<Error = MFA::Error>,
{
    type Error = MFA::Error;

    fn merge<'a>(
        &self,
        key: &[u8],
        values: &[Cow<'a, [u8]>],
    ) -> Result<Cow<'a, [u8]>, Self::Error> {
        match self {
            Either::Left(mfa) => mfa.merge(key, values),
            Either::Right(mfb) => mfb.merge(key, values),
        }
    }
}
