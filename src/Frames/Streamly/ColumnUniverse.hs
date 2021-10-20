{-# LANGUAGE AllowAmbiguousTypes, BangPatterns, CPP, ConstraintKinds, DataKinds,
             DerivingVia,
             FlexibleContexts, FlexibleInstances, GADTs, InstanceSigs,
             KindSignatures, LambdaCase, MultiParamTypeClasses,
             OverloadedStrings, QuasiQuotes, RankNTypes, RecordWildCards,
             ScopedTypeVariables, StandaloneKindSignatures,
             TemplateHaskell, TupleSections, TypeApplications,
             TypeFamilies, TypeOperators, UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Frames.Streamly.ColumnUniverse (
  CommonColumns
  , CommonColumnsCat
  , ParseHowRec
  , parseableParseHowRec
  , tryParseAll
  , CanParseAs(..)
  , ParseResult(..)
  , parseResult
  , parseResult'
  , ColType(..)
  , SomeMissing(..)
  , colTypeTH
  , colTypeSomeMissing
  , addParsedCell
  , initialColType
  , inferredToParseResult
) where

import Prelude hiding (Compose(..), Type, getConst, show, Const(..))
--import Text.Show (Show(..))
#if __GLASGOW_HASKELL__ < 808
import Data.Semigroup (Semigroup((<>)))
#endif
import qualified Data.Text as T
import Data.Vinyl
import Data.Vinyl.Functor
import Frames.Streamly.ColumnTypeable
import Frames.Streamly.Categorical
import Language.Haskell.TH

-- | Use a @ParseHow@ to (possibly) parse a given @Text@ as a value of type @a@
inferParseable :: ParseHow a -> T.Text -> (Maybe :. Parsed) a
inferParseable ParseHow{..} = Compose . phParse
{-# INLINE inferParseable #-}

-- | Helper to call 'inferParseablePH' on a 'Rec'.
inferParseable' :: ParseHow a -> (((->) T.Text) :. (Maybe :. Parsed)) a
inferParseable'  = Compose . inferParseable
{-# INLINE inferParseable' #-}

-- * Record Helpers

type ParseHowRec ts = Rec ParseHow ts
-- | Generate a Rec of @ParseHow@ for a list of types with 'Parseable' instances.
parseableParseHowRec :: RPureConstrained Parseable ts => ParseHowRec ts
parseableParseHowRec = rpureConstrained @Parseable parseableParseHow
{-# INLINEABLE parseableParseHowRec #-}

-- | try to parse each type in a type-list,, putting the result in a Rec.
tryParseAll :: forall ts. (RMap ts)
            => ParseHowRec ts -> T.Text -> Rec (Maybe :. Parsed) ts
tryParseAll phR = rtraverse getCompose funs
  where funs :: Rec (((->) T.Text) :. (Maybe :. Parsed)) ts
        funs = rmap inferParseable' phR
{-# INLINABLE tryParseAll #-}

-- * Common Columns

-- | Common column types: 'Bool', 'Int', 'Double', 'T.Text'
type CommonColumns = [Bool, Int, Double, T.Text]

-- | Common column types including categorical types.
type CommonColumnsCat = [Bool, Int, Double, Categorical 8, T.Text]

-- | Define a set of variants that captures all possible column types.
--type ColumnUniverse = CoRec ColInfo

-- | A universe of common column variants. These are the default
-- column types that @Frames@ can infer. See the
-- <http://acowley.github.io/Frames/#sec-4 Tutorial> for an example of
-- extending the default types with your own.
--type Columns = ColumnUniverse CommonColumns

-- @adamConnerSax new stuff
data SomeMissing = SomeMissing | NoneMissing deriving (Eq, Show)

instance Semigroup SomeMissing where
  SomeMissing <> _ = SomeMissing
  _ <> SomeMissing = SomeMissing
  NoneMissing <> NoneMissing = NoneMissing

data CanParseAs p = YesParse (Parsed p) | NoParse

-- this is semigroup like but should not escape this module because of dropping rhs value
-- so we just make it a function which we do not export
combineCanParseAs :: ParseHow p -> CanParseAs p -> CanParseAs p -> CanParseAs p
combineCanParseAs ParseHow{..} (YesParse x) (YesParse y) = maybe NoParse YesParse $ phParseCombine x y
combineCanParseAs _ _ _ = NoParse

data ParseResult ts = MissingData | ParseResult (Rec CanParseAs ts)

parseResult' :: (RecApplicative ts
                , RMap ts
                , RApply ts)
               => ParseHowRec ts -> (Text -> Bool) -> Text -> ParseResult ts
parseResult' phRec missingF t
  | missingF t = MissingData
  | otherwise = ParseResult $ recParsedToRecCanParseAs $ tryParseAll phRec t
{-# INLINEABLE parseResult' #-}

parseResult ::  (RecApplicative ts
                  , RMap ts
                  , RApply ts)
                => ParseHowRec ts -> Text -> ParseResult ts
parseResult phR = parseResult' phR defaultMissing where
  defaultMissing t = T.null t || t == "NA"
{-# INLINEABLE parseResult #-}

recParsedToRecCanParseAs :: RMap ts => Rec (Maybe :. Parsed) ts -> Rec CanParseAs ts
recParsedToRecCanParseAs = rmap (parsedToCanParseAs . getCompose)
{-# INLINEABLE recParsedToRecCanParseAs #-}

parsedToCanParseAs :: Maybe (Parsed a) -> CanParseAs a
parsedToCanParseAs Nothing = NoParse
parsedToCanParseAs (Just x)  = YesParse x
{-# INLINE parsedToCanParseAs #-}

data ColType ts = UnknownColType SomeMissing
                | KnownColType (SomeMissing, Rec CanParseAs ts)

type ColTH = Either (String -> Q [Dec]) Type
type ColTHF = Lift (->) CanParseAs (Const (Maybe ColTH))

colTHs :: RMap ts
       => ParseHowRec ts
       -> Rec (ColTHF) ts
colTHs = rmap f where
  f :: ParseHow a -> ColTHF a
  f ParseHow{..} =  Lift $ \x -> Const $ case x of
    NoParse ->  Nothing
    YesParse a -> Just $ phRepresentableAsType a
{-# INLINE colTHs #-}

fallbackText :: ColTH
fallbackText = Right (ConT (mkName "Text"))

instance ( RFoldMap ts
         , RMap ts
         , RApply ts
         , RecApplicative ts)
     => ColumnTypeable (ColType ts) where
  type ParseType (ColType ts) = ParseResult ts
  type Parsers (ColType ts) = ParseHowRec ts
  colType = colTypeTH
  {-# INLINEABLE colType #-}
  inferType = parseResult'
  {-# INLINEABLE inferType #-}
  initialColType = UnknownColType NoneMissing
  {-# INLINE initialColType #-}
  updateWithParse = addParsedCell
  {-# INLINE updateWithParse #-}

colTypeTH :: (RecApplicative ts
             , RFoldMap ts
             , RMap ts
             , RApply ts)
          => ParseHowRec ts -> ColType ts -> Either (String -> Q [Dec]) Type
colTypeTH phR t =  case t of
    UnknownColType _ -> fallbackText
    KnownColType (_,ts) ->
      fromMaybe fallbackText $ getFirst $ rfoldMap (First . getConst) $ rapply (colTHs phR) ts

colTypeSomeMissing :: ColType ts -> SomeMissing
colTypeSomeMissing (UnknownColType x) = x
colTypeSomeMissing (KnownColType (x, _)) = x

inferredToParseResult :: ColType ts -> ParseResult ts
inferredToParseResult (UnknownColType _) = MissingData
inferredToParseResult (KnownColType (_, x)) = ParseResult x
{-# INLINE inferredToParseResult #-}

addParsedCell :: (RMap ts, RApply ts) => ParseHowRec ts -> ColType ts -> ParseResult ts -> ColType ts
addParsedCell _ (UnknownColType _) MissingData = UnknownColType SomeMissing
addParsedCell _ (UnknownColType sm) (ParseResult pRec) = KnownColType (sm, pRec)
addParsedCell _ (KnownColType (_, ctRec)) MissingData = KnownColType (SomeMissing, ctRec)
addParsedCell phR (KnownColType (sm, ctRec)) (ParseResult pRec) = KnownColType (sm, newCtRec)
  where
    newCtRec = rzipWith3 combineCanParseAs phR pRec ctRec
{-# INLINEABLE addParsedCell #-}

rzipWith3 :: forall f g h q ts. (RMap ts, RApply ts)
          => (forall x. f x -> g x -> h x -> q x)
          -> Rec f ts
          -> Rec g ts
          -> Rec h ts
          -> Rec q ts
rzipWith3 f fs gs = rapply appliedFG where
  appliedFG :: Rec (Lift (->) h q) ts
  appliedFG = rzipWith liftFG fs gs where
    liftFG :: f x -> g x -> Lift (->) h q x
    liftFG fx gx = Lift $ f fx gx
{-# INLINEABLE rzipWith3 #-}

data SimpleDict c a where
  SimpleDict :: c a => SimpleDict c a

rzipWithC :: forall c ts f g h. (RMap ts, RApply ts, RPureConstrained c ts)
          => (forall x.c x => f x -> g x -> h x) -> Rec f ts -> Rec g ts -> Rec h ts
rzipWithC zipF t1 = rapply (g dicts t1)  where
  dicts :: Rec (SimpleDict c) ts
  dicts = rpureConstrained @c SimpleDict
  g :: Rec (SimpleDict c) ts -> Rec f ts -> Rec (Lift (->) g h) ts
  g cs cps = rzipWith h cs cps where
    h :: SimpleDict c a -> f a -> Lift (->) g h a
    h c x1 = case c of
      SimpleDict -> Lift $ \x2 -> zipF x1 x2
{-# INLINEABLE rzipWithC #-}
