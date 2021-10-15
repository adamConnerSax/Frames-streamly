{-# LANGUAGE AllowAmbiguousTypes, BangPatterns, CPP, ConstraintKinds, DataKinds,
             DerivingVia,
             FlexibleContexts, FlexibleInstances, GADTs, InstanceSigs,
             KindSignatures, LambdaCase, MultiParamTypeClasses,
             OverloadedStrings, QuasiQuotes, RankNTypes,
             ScopedTypeVariables, StandaloneKindSignatures,
             TemplateHaskell, TupleSections, TypeApplications,
             TypeFamilies, TypeOperators, UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Frames.Streamly.ColumnUniverse (
  CoRec{-, Columns, ColumnUniverse, ColInfo -}
  , CommonColumns, CommonColumnsCat {-,  parsedTypeRep -}
  , tryParseAll {-, bestRep-}
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
import Data.Vinyl.CoRec
import Data.Vinyl.Functor
--import Data.Vinyl.TypeLevel (RIndex, NatToInt)
import Frames.Streamly.ColumnTypeable
import Frames.Streamly.Categorical
import Language.Haskell.TH

-- | Extract a function to test whether some value of a given type
-- could be read from some 'T.Text'.
inferParseable :: Parseable a => T.Text -> (Maybe :. Parsed) a
inferParseable = Compose . parse

-- | Helper to call 'inferParseable' on variants of a 'CoRec'.
inferParseable' :: Parseable a => (((->) T.Text) :. (Maybe :. Parsed)) a
inferParseable' = Compose inferParseable

-- * Record Helpers

tryParseAll :: forall ts. (RecApplicative ts, RPureConstrained Parseable ts)
            => T.Text -> Rec (Maybe :. Parsed) ts
tryParseAll = rtraverse getCompose funs
  where funs :: Rec (((->) T.Text) :. (Maybe :. Parsed)) ts
        funs = rpureConstrained @Parseable inferParseable'

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
{-
data CanParseAs p where
  YesParse :: Parseable p => Parsed p -> CanParseAs p
  NoParse :: CanParseAs p
-}
{-
instance Functor CanParseAs where
  fmap f (YesParse p) = YesParse (fmap f p)
  fmap _ NoParse = NoParse
-}

-- this is semigroup like but should not escape this module because of dropping rhs value
-- so we just make it a function which we do not export
combineCanParseAs :: Parseable p => CanParseAs p -> CanParseAs p -> CanParseAs p
combineCanParseAs (YesParse x) (YesParse y) = maybe NoParse YesParse $ parseCombine x y
combineCanParseAs _ _ = NoParse

data ParseResult ts = MissingData | ParseResult (Rec CanParseAs ts)

parseResult' :: (RecApplicative ts
                , RMap ts
                , RApply ts
                , RPureConstrained Parseable ts)
             => (Text -> Bool) -> Text -> ParseResult ts
parseResult' missingF t
  | missingF t = MissingData
  | otherwise = ParseResult $ recParsedToRecCanParseAs $ tryParseAll t
{-# INLINEABLE parseResult' #-}

parseResult ::  (RecApplicative ts
                , RMap ts
                , RApply ts
                , RPureConstrained Parseable ts)
                => Text -> ParseResult ts
parseResult = parseResult' defaultMissing where
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

colTHs :: (RecApplicative ts
          , RPureConstrained Parseable ts)
       => Rec (ColTHF) ts
colTHs = rpureConstrained @Parseable f where
  f :: forall a.Parseable a => ColTHF a --CanParseAs a -> (Const ColTH) a
  f = Lift $ \x -> Const $ case x of
    NoParse ->  Nothing
    YesParse a -> Just $ getConst $ representableAsType a
{-# INLINE colTHs #-}

fallbackText :: ColTH
fallbackText = Right (ConT (mkName "Text"))


instance ( RFoldMap ts
         , RMap ts
         , RApply ts
         , RecApplicative ts
         , RPureConstrained Parseable ts) => ColumnTypeable (ColType ts) where
  colType = colTypeTH
  {-# INLINEABLE colType #-}
  inferType isMissing t = case parseResult' isMissing t of
    MissingData -> UnknownColType SomeMissing
    ParseResult x -> KnownColType (NoneMissing, x)
  {-# INLINEABLE inferType #-}


colTypeTH :: (RecApplicative ts
             , RFoldMap ts
             , RApply ts
             ,  RPureConstrained Parseable ts)
          => ColType ts -> Either (String -> Q [Dec]) Type
colTypeTH t =  case t of
    UnknownColType _ -> fallbackText
    KnownColType (_,ts) ->
      fromMaybe fallbackText $ getFirst $ rfoldMap (First . getConst) $ rapply colTHs ts

colTypeSomeMissing :: ColType ts -> SomeMissing
colTypeSomeMissing (UnknownColType x) = x
colTypeSomeMissing (KnownColType (x, _)) = x

inferredToParseResult :: ColType ts -> ParseResult ts
inferredToParseResult (UnknownColType _) = MissingData
inferredToParseResult (KnownColType (_, x)) = ParseResult x
{-# INLINE inferredToParseResult #-}

initialColType :: ColType ts
initialColType = UnknownColType NoneMissing
{-# INLINE initialColType #-}


addParsedCell :: (RMap ts, RApply ts, RPureConstrained Parseable ts) => ColType ts -> ParseResult ts -> ColType ts
addParsedCell (UnknownColType _) MissingData = UnknownColType SomeMissing
addParsedCell (UnknownColType sm) (ParseResult pRec) = KnownColType (sm, pRec)
addParsedCell (KnownColType (_, ctRec)) MissingData = KnownColType (SomeMissing, ctRec)
addParsedCell (KnownColType (sm, ctRec)) (ParseResult pRec) = KnownColType (sm, newCtRec)
  where
    newCtRec = rzipWithC @Parseable combineCanParseAs pRec ctRec
{-# INLINEABLE addParsedCell #-}

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

{-
-- * Column Type Inference

-- | Information necessary for synthesizing row types and comparing
-- types.
newtype ColInfo a = ColInfo (Either (String -> Q [Dec]) Type, Parsed a)
instance Show a => Show (ColInfo a) where
  show (ColInfo (t,p)) = "(ColInfo {"
                         ++ either (const "cat") show t
                         ++ ", "
                         ++ show (discardConfidence p) ++"})"

parsedToColInfo :: Parseable a => Parsed a -> ColInfo a
parsedToColInfo x = case getConst rep of
                      Left dec -> ColInfo (Left dec, x)
                      Right ty ->
                        ColInfo (Right ty, x)
  where rep = representableAsType x

parsedTypeRep :: ColInfo a -> Parsed Type
parsedTypeRep (ColInfo (t,p)) =
  const (either (const (ConT (mkName "Categorical"))) id t) <$> p

-- | Map 'Type's we know about (with a special treatment of
-- synthesized types for categorical variables) to 'Int's for ordering
-- purposes.
orderParsePriorities :: Parsed (Maybe Type) -> Maybe Int
orderParsePriorities x =
  case discardConfidence x of
    Nothing -> Just 1 -- categorical variable
    Just t
      | t == tyText -> Just (0 + uncertainty)
      | t == tyDbl -> Just (2 + uncertainty)
      | t == tyInt -> Just (3 + uncertainty)
      | t == tyBool -> Just (4 + uncertainty)
      | otherwise -> Nothing
  where tyText = ConT (mkName "Text")
        tyDbl = ConT (mkName "Double")
        tyInt = ConT (mkName "Int")
        tyBool = ConT (mkName "Bool")
        uncertainty = case x of Definitely _ -> 0; Possibly _ -> 5

-- | We use a join semi-lattice on types for representations. The
 -- bottom of the lattice is effectively an error (we have nothing to
-- represent), @Bool < Int@, @Int < Double@, and @forall n. n <= Text@.
--
-- The high-level goal here is that we will pick the "greater" of two
-- choices in 'bestRep'. A 'Definitely' parse result is preferred over
-- a 'Possibly' parse result. If we have two distinct 'Possibly' parse
-- results, we give up. If we have two distinct 'Definitely' parse
-- results, we are in dangerous waters: all data is parseable at
-- /both/ types, so which do we default to? The defaulting choices
-- made here are described in the previous paragraph. If there is no
-- defaulting rule, we give up (i.e. use 'T.Text' as a
-- representation).
lubTypes :: Parsed (Maybe Type) -> Parsed (Maybe Type) -> Maybe Ordering
lubTypes x y = compare <$> orderParsePriorities y <*> orderParsePriorities x

instance (T.Text ∈ ts, RPureConstrained Parseable ts) => Monoid (CoRec ColInfo ts) where
  mempty = CoRec (ColInfo ( Right (ConT (mkName "Text")), Possibly T.empty))
  mappend x y = x <> y

-- | A helper For the 'Semigroup' instance below.
mergeEqTypeParses :: forall ts. (RPureConstrained Parseable ts, T.Text ∈ ts)
                  => CoRec ColInfo ts -> CoRec ColInfo ts -> CoRec ColInfo ts
mergeEqTypeParses x@(CoRec _) y = fromMaybe definitelyText
                                $ coRecTraverse getCompose
                                                (coRecMapC @Parseable aux x)
  where definitelyText = CoRec (ColInfo (Right (ConT (mkName "Text")), Definitely T.empty))
        aux :: forall a. (Parseable a, NatToInt (RIndex a ts))
            => ColInfo a -> (Maybe :. ColInfo) a
        aux (ColInfo (_, pX)) =
          case asA' @a y of
            Nothing -> Compose Nothing
            Just (ColInfo (_, pY)) ->
              maybe (Compose Nothing)
                    (Compose . Just . parsedToColInfo)
                    (parseCombine pX pY)

instance (T.Text ∈ ts, RPureConstrained Parseable ts)
  => Semigroup (CoRec ColInfo ts) where
  x@(CoRec (ColInfo (tyX, pX))) <> y@(CoRec (ColInfo (tyY, pY))) =
    case lubTypes (const (either (const Nothing) Just tyX) <$> pX)
                  (const (either (const Nothing) Just tyY) <$> pY) of
      Just GT -> x
      Just LT -> y
      Just EQ -> mergeEqTypeParses x y
      Nothing -> mempty

-- | Find the best (i.e. smallest) 'CoRec' variant to represent a
-- parsed value. For inspection in GHCi after loading this module,
-- consider this example:
--
-- >>> :set -XTypeApplications
-- >>> :set -XOverloadedStrings
-- >>> import Data.Vinyl.CoRec (foldCoRec)
-- >>> foldCoRec parsedTypeRep (bestRep @CommonColumns "2.3")
-- Definitely Double
bestRep :: forall ts.
           (RPureConstrained Parseable ts,
            FoldRec ts ts,
            RecApplicative ts, T.Text ∈ ts)
        => T.Text -> CoRec ColInfo ts
bestRep t
  | T.null t || t == "NA" = (CoRec (parsedToColInfo (Possibly T.empty)))
  | otherwise = coRecMapC @Parseable parsedToColInfo
              . fromMaybe (CoRec (Possibly T.empty :: Parsed T.Text))
              . firstField -- choose first non-Nothing field
              . (tryParseAll :: T.Text -> Rec (Maybe :. Parsed) ts)
              $ t
{-# INLINABLE bestRep #-}

instance (RPureConstrained Parseable ts, FoldRec ts ts,
          RecApplicative ts, T.Text ∈ ts) =>
    ColumnTypeable (CoRec ColInfo ts) where
  colType (CoRec (ColInfo (t, _))) = t
  {-# INLINE colType #-}
  inferType = bestRep
  {-# INLINABLE inferType #-}

#if !MIN_VERSION_vinyl(0,11,0)
instance forall ts. (RPureConstrained Show ts, RecApplicative ts)
  => Show (CoRec ColInfo ts) where
  show x = "(Col " ++ onCoRec @Show show x ++")"
#endif
-}
