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
import qualified Data.Text as T
import Data.Vinyl
import Data.Vinyl.Functor
import Frames.Streamly.ColumnTypeable
import Frames.Streamly.Categorical
import Language.Haskell.TH
import Frames.Streamly.TH (RowGen(columnPa))
import GHC.ForeignPtr (Finalizers(HaskellFinalizers))
import Data.Primitive (Prim(sizeOf#))

-- | Use a @ParseHow@ to (possibly) parse a given @Text@ as a value of type @a@
inferParseable :: ParseHow a -> T.Text -> (Maybe :. Parsed) a
inferParseable ParseHow{..} = Compose . phParse
{-# INLINE inferParseable #-}

-- | Helper to call 'inferParseable' on a 'Rec'.
inferParseable' :: ParseHow a -> (((->) T.Text) :. (Maybe :. Parsed)) a
inferParseable'  = Compose . inferParseable
{-# INLINE inferParseable' #-}

-- * Record Helpers

type ParseHowRec ts = Rec ParseHow ts
-- | Generate a Rec of @ParseHow@ for a list of types with 'Parseable' instances.
parseableParseHowRec :: RPureConstrained Parseable ts => ParseHowRec ts
parseableParseHowRec = rpureConstrained @Parseable parseableParseHow
{-# INLINEABLE parseableParseHowRec #-}

-- | Try to parse the given 'Text' to each type in a type-list, putting the result in a Rec.
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
-- NB: If @Categorical 8@ is too small for some types you can
-- increase the number or *add* a larger 'Categorical' after. e.g.,
-- @MoreColumnsCat = [Bool, Int, Double, Categorical 8, Categorical 32, T.Text]@
type CommonColumnsCat = [Bool, Int, Double, Categorical 8, T.Text]

-- | A universe of common column variants. These are the default
-- column types that @Frames@ can infer. See the
-- <http://acowley.github.io/Frames/#sec-4 Tutorial> for an example of
-- extending the default types with your own.
--type Columns = ColumnUniverse CommonColumns

-- @adamConnerSax new stuff
-- | Type to flag whether some data in a column is missing.
data SomeMissing = SomeMissing | NoneMissing deriving (Eq, Show)

-- | Type to hold the result of an attempt to parse at each possible type.
-- Isomorphic to @Maybe (Parsed p)@ but makes the code clearer.
data CanParseAs p = YesParse (Parsed p) | NoParse

-- | Given the 'ParseHow' record-of-functions for @p@, combine two possible
-- parses of @p@ into a possible parse of @p@.  Largely, this amounts to
-- success if both succeeded and failure if either failed.  But for some types,
-- e.g., 'Categorical` two previous parses can succeed but the combination can fail.
-- In the case of `Categorical` that can happen because we discover, for example, a
-- 9th alternative when we are trying to parse as @Categorical 8@. In general, this
-- could be useful anytime the success or failure of parsing a column as type @p@
-- depends on some property of the set of all values present in the column rather
-- than just if each single parse attempt succeeds.
combineCanParseAs :: ParseHow p -> CanParseAs p -> CanParseAs p -> CanParseAs p
combineCanParseAs ParseHow{..} (YesParse x) (YesParse y) = maybe NoParse YesParse $ phParseCombine x y
combineCanParseAs _ _ _ = NoParse

-- | Holder for the results of attempting to parse as all the types
-- in the type-list @ts@.  Isomorphic to @Maybe (Rec CanParseAs ts)@
data ParseResult ts = MissingData | ParseResult (Rec CanParseAs ts)

-- | Given a 'ParseHow' for each type in @ts@ and a function to indicate which data is "missing"
-- attempt to parse a given 'Text' as each type in @ts@ and return the result as possible
-- parses at each type.
parseResult' :: RMap ts => ParseHowRec ts -> (Text -> Bool) -> Text -> ParseResult ts
parseResult' phRec missingF t
  | missingF t = MissingData
  | otherwise = ParseResult $ recParsedToRecCanParseAs $ tryParseAll phRec t
{-# INLINEABLE parseResult' #-}

-- | Given a 'ParseHow' for each type in @ts@,
-- attempt to parse a given 'Text' as each type in @ts@ and return the result as possible
-- parses at each type.
-- NB: Uses a default definition of "missing" data: "" or "NA"
parseResult ::  RMap ts => ParseHowRec ts -> Text -> ParseResult ts
parseResult phR = parseResult' phR defaultMissing where
  defaultMissing t = T.null t || t == "NA"
{-# INLINEABLE parseResult #-}


-- | Helper for converting @Maybe :. Parsed@ to  'CanParseAs'
recParsedToRecCanParseAs :: RMap ts => Rec (Maybe :. Parsed) ts -> Rec CanParseAs ts
recParsedToRecCanParseAs = rmap (parsedToCanParseAs . getCompose)
{-# INLINEABLE recParsedToRecCanParseAs #-}

-- | Helper for converting @Maybe (Parsed a)@ to  @CanParseAs a@
parsedToCanParseAs :: Maybe (Parsed a) -> CanParseAs a
parsedToCanParseAs Nothing = NoParse
parsedToCanParseAs (Just x)  = YesParse x
{-# INLINE parsedToCanParseAs #-}

-- | Type to hold parsing information for a single column
-- It starts completely unknown (@UnknownColType NoneMissing@)
-- and remains completely unknown if the data starts off missing
-- (@UnknownColType SomeMissing@).  Once we begin seeing non-missing
-- items we hold those results and whether or not we have encountered
-- any missing data, in @KnownColType@
data ColType ts = UnknownColType SomeMissing
                | KnownColType (SomeMissing, Rec CanParseAs ts)

type ColTH = Either (String -> Q [Dec]) Type
type ColTHF = Lift (->) CanParseAs (Const (Maybe ColTH))

-- | Given a 'Rec' of 'parseHow', build a @Rec@ of functions
-- which will map parse results for each type to the template-haskell
-- required to declare that type.
colTHs :: RMap ts
       => ParseHowRec ts
       -> Rec (ColTHF) ts
colTHs = rmap f where
  f :: ParseHow a -> ColTHF a
  f ParseHow{..} =  Lift $ \x -> Const $ case x of
    NoParse ->  Nothing
    YesParse a -> Just $ phRepresentableAsType a
{-# INLINE colTHs #-}

-- | Our default is always to declare a column as 'Text'
-- so we fall back to this in various situations where the
-- correct type is unclear.
fallbackText :: ColTH
fallbackText = Right (ConT (mkName "Text"))
{-# INLINE fallbackText #-}

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

-- | Given a 'Rec' of 'ParseHow' and the inference information in @ColType ts@,
-- produce the the template-haskell for the first type in the type-list @ts@ where
-- parsing succeeded.
-- This suggests an ordering of types in the list from most-to-least specific with
-- 'Text' always last.
-- NB: When using 'Categorical' this ordering requires some thought!  If you want
-- 'Categorical' for alternatives which cannot be parsed as anything but 'Text', you
-- should put your `Categorical'(s) (in increasing order of size) right before 'Text'
-- at the end of the list.  But if you have columns using integers to code Categorical
-- values you might want a Categorical *before* 'Int'.
colTypeTH :: (RecApplicative ts
             , RFoldMap ts
             , RMap ts
             , RApply ts)
          => ParseHowRec ts -> ColType ts -> Either (String -> Q [Dec]) Type
colTypeTH phR t =  case t of
    UnknownColType _ -> fallbackText
    KnownColType (_,ts) ->
      fromMaybe fallbackText $ getFirst $ rfoldMap (First . getConst) $ rapply (colTHs phR) ts
{-# INLINEABLE colTypeTH #-}

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

-- | rzipWith extended to 3 argument functions
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
    liftFG fx  = Lift . f fx
{-# INLINEABLE rzipWith3 #-}

data SimpleDict c a where
  SimpleDict :: c a => SimpleDict c a

-- | rzipWith but with a constraint in-scope when the zipping function is applied.
rzipWithC ::forall c ts f g h. (RMap ts, RApply ts, RPureConstrained c ts)
          => (forall x.c x => f x -> g x -> h x)
          -> Rec f ts
          -> Rec g ts
          -> Rec h ts
rzipWithC l = rzipWith3 q (rpureConstrained @c SimpleDict) where
  q :: SimpleDict c x -> f x -> g x -> h x
  q SimpleDict = l
{-# INLINEABLE rzipWithC #-}
