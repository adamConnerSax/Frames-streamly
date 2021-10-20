{-# LANGUAGE AllowAmbiguousTypes, BangPatterns,
             DefaultSignatures, DeriveFunctor,
             FlexibleInstances,
             LambdaCase,
             MultiParamTypeClasses, RankNTypes, ScopedTypeVariables
             , TemplateHaskell, TypeFamilies #-}
module Frames.Streamly.ColumnTypeable where

import Prelude hiding (Const(..), Type)
import Frames.Streamly.OrMissing

import Data.Readable (Readable(fromText))
import Data.Typeable (typeRep)
import qualified Data.Text as T
import Data.Vinyl.Functor (Const(..))
import Language.Haskell.TH

-- | Type to represent the result of successfully parsing a @Text@ to @a@.
-- Usually will be @Definitely@ but @Possibly@ may be used if further
-- parsing to type @a@ may show that @a@ was not possible type for the
-- entire column.  Used for the @Categorical@ type.
data Parsed a = Possibly a | Definitely a deriving (Eq, Ord, Show, Functor)

-- | Values that can be read from a 'T.Text' with more or less
-- discrimination.
class Parseable a where
  -- | E.g., if @m@ is 'Maybe',
  -- Returns 'Nothing' if a value of the given type can not be read;
  -- returns 'Just Possibly' if a value can be read, but is likely
  -- ambiguous (e.g. an empty string); returns 'Just Definitely' if a
  -- value can be read and is unlikely to be ambiguous.
  parse :: MonadPlus m => T.Text -> m (Parsed a)
  default parse :: (Readable a, MonadPlus m)
                => T.Text -> m (Parsed a)
  parse = fmap Definitely . fromText
  {-# INLINE parse #-}

  -- | Combine two parse results such that the combination can
  -- fail. Useful when we have two 'Possibly' parsed values that are
  -- different enough to suggest the parse of each should be
  -- considered a failure. The default implementation is to 'return'
  -- the first argument.
  parseCombine :: MonadPlus m => Parsed a -> Parsed a -> m (Parsed a)
  default parseCombine :: MonadPlus m => Parsed a -> Parsed a -> m (Parsed a)
  parseCombine = const . return
  {-# INLINE parseCombine #-}

  representableAsType :: Parsed a -> Const (Either (String -> Q [Dec]) Type) a
  default
    representableAsType :: Typeable a
                        => Parsed a -> Const (Either (String -> Q [Dec]) Type) a
  representableAsType =
    const (Const (Right (ConT (mkName (show (typeRep (Proxy :: Proxy a)))))))
  {-# INLINABLE representableAsType #-}


-- | Discard any estimate of a parse's ambiguity.
discardConfidence :: Parsed a -> a
discardConfidence (Possibly x) = x
discardConfidence (Definitely x) = x
{-# INLINE discardConfidence #-}

-- | Acts just like 'fromText': tries to parse a value from a 'T.Text'
-- and discards any estimate of the parse's ambiguity.
parse' :: (MonadPlus m, Parseable a) => T.Text -> m a
parse' = fmap discardConfidence . parse
{-# INLINE parse' #-}

parseIntish :: (Readable a, MonadPlus f) => T.Text -> f (Parsed a)
parseIntish t =
  Definitely <$> fromText (fromMaybe t (T.stripSuffix (T.pack ".0") t))
{-# INLINEABLE parseIntish #-}

instance Parseable Bool where

instance Parseable Int where
  parse = parseIntish
  {-# INLINE parse #-}

instance Parseable Int32 where
  parse = parseIntish
  {-# INLINE parse #-}

instance Parseable Int64 where
  parse = parseIntish
  {-# INLINE parse #-}

instance Parseable Integer where
  parse = parseIntish
  {-# INLINE parse #-}

instance Parseable Float where
instance Parseable Double where
  -- Some CSV's export Doubles in a format like '1,000.00', filtering
  -- out commas lets us parse those sucessfully
  parse = fmap Definitely . fromText . T.filter (/= ',')
  {-# INLINE parse #-}

instance Parseable T.Text where

-- @adamConnerSax new/changed stuff

-- | This class relates a universe of possible column types to Haskell
-- types, and provides a mechanism to infer which type best represents
-- some textual data.
-- The type @a@ must somehow represent the list of all possible inferred types and
-- which of those are currently possible given the data seen so far.
class ColumnTypeable a where
  type ParseType a
  -- ^ An associated type to represent the result of parsing a single @Text@ into possible column types
  -- represented by @a@
  type Parsers a
  -- ^ An associated type to represent information required to parse @Text@ to
  -- possible types all held in @a@, combine the current slate of possible types and
  -- a newly parsed set of possibilities and produce template haskell to declare the
  -- "best" (most specific) type in @a@.
  colType :: Parsers a -> a -> Either (String -> Q [Dec]) Type
  -- ^ TH haskell representing the best type among those possible in @a@.
  -- Used when declaring the type after inference.
  inferType :: Parsers a -> (T.Text -> Bool) -> T.Text -> ParseType a
  -- ^ Given parsing info and a function to indicate "missing" data, parse a @Text@ to possible types @a@
  initialColType :: a
  -- ^ A value of type @a@ to be used as the initial state in iterative column type inference.
  -- Usually indicates all types are still possible.
  updateWithParse :: Parsers a -> a -> ParseType a -> a
  -- ^ given parsing info, an @a@ representing possible column types given the data so far,
  -- and a parsed value, indicating possible types of the current item of data, update the
  -- possible types for the column.


instance (Typeable a, Parseable a) => Parseable (OrMissing a) where
  parse t = return $ maybe (Definitely Missing) (fmap Present) $ parse t
  {-# INLINEABLE parse #-}
  parseCombine p1 p2 = case (commute p1, commute p2) of
    (Missing, Missing) -> return $ Definitely Missing
    (Present x, Missing) -> return $ fmap Present x
    (Missing, Present x) -> return $ fmap Present x
    (Present x, Present y) -> fmap Present <$> parseCombine x y
    where
      commute :: Parsed (OrMissing a) -> OrMissing (Parsed a)
      commute (Possibly Missing) = Missing
      commute (Possibly (Present a)) = Present (Possibly a)
      commute (Definitely Missing) = Missing
      commute (Definitely (Present a)) = Present (Definitely a)
  {-# INLINEABLE parseCombine #-}

-- | Record-of-functions for column parsing.
data ParseHow a = ParseHow
  { phParse :: forall m. MonadPlus m =>  T.Text -> m (Parsed a)
    -- ^ attempt to parse the given 'Text' into the type @a@
  , phParseCombine ::  forall m. MonadPlus m => Parsed a -> Parsed a -> m (Parsed a)
  -- ^ check that two parsed values of type a are both consistent with being of type @a@
  , phRepresentableAsType :: Parsed a -> Either (String -> Q [Dec]) Type
  -- ^ Template-Haskell for declaring type @a@.  Either a function to produce a set of
  -- declarations (for 'Categorical') or the template-haskell 'Type' which can easily be
  -- turned into a declaration.
  }

-- | Generate a 'ParseHow' for a any type with a 'Parseable' instance.
parseableParseHow :: Parseable a => ParseHow a
parseableParseHow = ParseHow parse parseCombine (getConst . representableAsType)
{-# INLINEABLE parseableParseHow #-}

-- | Generate a 'ParseHow' for any type with a 'Typeable' instance and a parsing function  of
-- the form @Text -> Maybe a@.
simpleParseHow :: forall a . Typeable a => (Text -> Maybe a) -> ParseHow a
simpleParseHow g = ParseHow p c r where
  p :: forall n. MonadPlus n =>  T.Text -> n (Parsed a) -- this sig required for MonadPlus constraint
  p = maybe mzero (return . Definitely) . g
  c pa _ = return pa
  r _ = Right (ConT (mkName (show (typeRep (Proxy :: Proxy a)))))
{-# INLINEABLE simpleParseHow #-}
