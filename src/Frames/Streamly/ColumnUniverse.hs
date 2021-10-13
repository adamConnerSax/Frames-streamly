{-# LANGUAGE BangPatterns, CPP, ConstraintKinds, DataKinds,
             DerivingVia,
             FlexibleContexts, FlexibleInstances, GADTs, InstanceSigs,
             KindSignatures, LambdaCase, MultiParamTypeClasses,
             OverloadedStrings, QuasiQuotes, RankNTypes,
             ScopedTypeVariables, StandaloneKindSignatures,
             TemplateHaskell, TypeApplications,
             TypeFamilies, TypeOperators, UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Frames.Streamly.ColumnUniverse (
  CoRec, Columns, ColumnUniverse, ColInfo,
  CommonColumns, CommonColumnsCat, parsedTypeRep
  , tryParseAll, bestRep
) where

import Prelude hiding (Compose(..), Type, getConst, show)
import Text.Show (Show(..))
--import Data.Maybe (fromMaybe)
#if __GLASGOW_HASKELL__ < 808
import Data.Semigroup (Semigroup((<>)))
#endif
import qualified Data.Text as T
import Data.Vinyl
import Data.Vinyl.CoRec
import Data.Vinyl.Functor
import Data.Vinyl.TypeLevel (RIndex, NatToInt)
import Frames.Streamly.ColumnTypeable
import Frames.Melt (ElemOf)
import Frames.Categorical
import Language.Haskell.TH
import qualified Data.Kind as Kind

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

-- * Common Columns

-- | Common column types: 'Bool', 'Int', 'Double', 'T.Text'
type CommonColumns = [Bool, Int, Double, T.Text]

-- | Common column types including categorical types.
type CommonColumnsCat = [Bool, Int, Double, Categorical 8, T.Text]

-- | Define a set of variants that captures all possible column types.
type ColumnUniverse = CoRec ColInfo

-- | A universe of common column variants. These are the default
-- column types that @Frames@ can infer. See the
-- <http://acowley.github.io/Frames/#sec-4 Tutorial> for an example of
-- extending the default types with your own.
type Columns = ColumnUniverse CommonColumns


-- @adamConnerSax new stuff
data SomeMissing = SomeMissing | NoneMissing deriving (Show)

instance Semigroup SomeMissing where
  SomeMissing <> _ = SomeMissing
  _ <> SomeMissing = SomeMissing
  NoneMissing <> NoneMissing = NoneMissing

data CanParseAs a = YesParse SomeMissing | NoParse SomeMissing

instance Functor CanParseAs where
  fmap _ (YesParse x) = YesParse x
  fmap _ (NoParse x) = NoParse x

instance Semigroup (CanParseAs a) where
  YesParse x <> YesParse y = YesParse (x <> y)
  YesParse x <> NoParse y = NoParse (x <> y)
  NoParse x <> YesParse y = NoParse (x <> y)
  NoParse x <> NoParse y = NoParse (x <> y)

instance (RMap ts, RApply ts) => Semigroup (Rec CanParseAs ts) where
  r1 <> r2 = rzipWith (<>) r1 r2


newtype ParseResult ts = ParseResult (Rec CanParseAs ts)
--  deriving Semigroup via newtype

instance (RMap ts, RApply ts) => Semigroup (ParseResult ts) where
  (ParseResult x) <> (ParseResult y) = ParseResult $ x <> y

instance (RMap ts, RApply ts, RecApplicative ts) => Monoid (ParseResult ts) where
  mempty = ParseResult $ rpure (YesParse SomeMissing)
  mappend = (<>)

--type ParseResult ts = Rec CanParseAs ts

parseResult' :: (RecApplicative ts
                , RMap ts
                , RApply ts
                , RPureConstrained Parseable ts)
             => (Text -> Bool) -> Text -> ParseResult ts
parseResult' missingF t
  | missingF t == True = mempty
  | otherwise = ParseResult $ recParsedToRecCanParseAs $ tryParseAll t

parseResult ::  (RecApplicative ts
                , RMap ts
                , RApply ts
                , RPureConstrained Parseable ts)
                => Text -> ParseResult ts
parseResult = parseResult' defaultMissing where
  defaultMissing t = T.null t || t == "NA"


recParsedToRecCanParseAs :: RMap ts => Rec (Maybe :. Parsed) ts -> Rec CanParseAs ts
recParsedToRecCanParseAs = rmap (parsedToCanParseAs . getCompose)

parsedToCanParseAs :: Maybe (Parsed a) -> CanParseAs a
parsedToCanParseAs Nothing = NoParse NoneMissing
parsedToCanParseAs (Just _)  = YesParse NoneMissing



{-
reTypeSomeMissing :: SomeMissing () -> SomeMissing a
reTypeSomeMissing SomeMissing = SomeMissing
reTypeSomeMissing NoneMissing = NoneMissing
-}

--newtype ColumnInfo a = ColumnInfo (Either (String -> Q [Dec]) Type, SomeMissing a)
type ColType ts = Rec CanParseAs ts

--addParsedCell :: ColType ts -> ParseResult ts -> ColType ts


{-
type InferJoin :: Kind.Type -> Kind.Type -> Kind.Type
type family InferJoin a b

type instance InferJoin a a = a
type instance (Parseable a, Parseable b) => InferJoin a b =

type instance InferJoin _ Text = Text
type instance InferJoin Text _ = Text
type instance InferJoin _ _ = Text
-}
{-
inferCellInContext :: forall ts.  (FoldRec ts ts, RPureConstrained Parseable ts, ElemOf ts T.Text)
                   => ColType ts -> ParseResult ts -> ColType ts
inferCellInContext (UnknownColType _) MissingData = UnknownColType SomeMissing
inferCellInContext (UnknownColType sm) (ParsedRec x) = KnownColType $ parseRecToColInfoMCoRec sm x
inferCellInContext (KnownColType x) MissingData = KnownColType $ coRecMap f x where
  f :: ColInfoM a -> ColInfoM a
  f (ColInfoM (t, _)) = ColInfoM (t, SomeMissing)

-- We have a column with an inferred type and a parsed cell with
inferCellInContext (KnownColType cx) (ParsedRec y) =
-}

  {-
  KnownColType $ mergeCoRecs cx (parseRecToColInfoMCoRec NoneMissing y) where
  mergeCoRecs :: CoRec ColInfoM ts -> CoRec ColInfoM ts -> CoRec ColInfoM ts
  mergeCoRecs crX@(CoRec (ColInfoM ) crY = fromMaybe fallbackText
                        $ coRecTraverse getCompose (coRecMapC @Parseable aux crX)
    where
      fallbackText :: CoRec ColInfoM ts
      fallbackText = CoRec @Text @ts (ColInfoM (Right (ConT (mkName "Text")), NoneMissing))
      aux :: forall a. (Parseable a, NatToInt (RIndex a ts)) => ColInfoM a -> (Maybe :. ColInfoM) a
      aux (ColInfoM (_, sm)) = case asA' @a crY of
        Nothing -> Compose Nothing
        Just ((ColInfoM (t, sm'))) -> Compose $ Just $ ColInfoM (t, sm <> sm')
-}
{-
parsedRecAsCoRec ::  (FoldRec ts ts, RPureConstrained Parseable ts, ElemOf ts T.Text) => Rec (Maybe :. Parsed) ts -> CoRec Parsed ts
parsedRecAsCoRec = fromMaybe (CoRec (Possibly T.empty :: Parsed T.Text)) . firstField

parsedToColInfoM :: Parseable a => SomeMissing a -> Parsed a -> ColInfoM a
parsedToColInfoM sm x = ColInfoM (getConst $ representableAsType x, sm)

parsedCoRecToColInfoM :: RPureConstrained Parseable ts => SomeMissing () -> CoRec Parsed ts -> CoRec ColInfoM ts
parsedCoRecToColInfoM sm = coRecMapC @Parseable (parsedToColInfoM $ reTypeSomeMissing sm)

parseRecToColInfoMCoRec :: (FoldRec ts ts, RPureConstrained Parseable ts, ElemOf ts T.Text) => SomeMissing () -> Rec (Maybe :. Parsed) ts -> CoRec ColInfoM ts
parseRecToColInfoMCoRec sm = parsedCoRecToColInfoM sm . parsedRecAsCoRec
-}
