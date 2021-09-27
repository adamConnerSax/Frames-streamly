{-# LANGUAGE DeriveLift #-}
{-# Language GADTs #-}
{-|
Module      : Frames.Streamly.Internal.CSV
Description : Internal module to make the HeaderList opaque
Copyright   : (c) Adam Conner-Sax 2021
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

-}
module Frames.Streamly.Internal.CSV where

import qualified Data.Map as Map
import qualified Data.Set as Set
import Language.Haskell.TH.Syntax (Lift)

data ColumnState = Exclude | Include Text deriving (Eq, Lift)

-- For RowGen

data RowGenColumnHandler a where
  GenUsingHeader :: (Text -> ColumnState) -> RowGenColumnHandler Text
  GenIgnoringHeader :: (Int -> ColumnState) -> RowGenColumnHandler Int
  GenWithoutHeader :: (Int -> ColumnState) -> RowGenColumnHandler Int

{-
columnStateFunction :: RowGenColumnHandler a -> (a -> ColumnState)
columnStateFunction (UseHeader f) = f
columnStateFunction (IgnoreHeader f) = f
columnStateFunction (NoHeader f) = f
-}

modifyColumnStateFunction :: RowGenColumnHandler a -> ((a -> ColumnState) -> (a -> ColumnState)) -> RowGenColumnHandler a
modifyColumnStateFunction (GenUsingHeader f) g = GenUsingHeader $ g f
modifyColumnStateFunction (GenIgnoringHeader f) g = GenIgnoringHeader $ g f
modifyColumnStateFunction (GenWithoutHeader f) g = GenWithoutHeader $ g f

allColumnsAsNamed :: RowGenColumnHandler Text
allColumnsAsNamed = GenUsingHeader Include

noHeaderColumnsNumbered' :: RowGenColumnHandler Int
noHeaderColumnsNumbered' = GenWithoutHeader $ \n -> Include $ show n

prefixColumns :: Text -> RowGenColumnHandler a -> RowGenColumnHandler a
prefixColumns p ch = modifyColumnStateFunction ch g where
  g f = \x -> case f x of
    Exclude -> Exclude
    Include t -> Include $ p <> t

noHeaderColumnsNumbered :: Text -> RowGenColumnHandler Int
noHeaderColumnsNumbered prefix = prefixColumns prefix $ noHeaderColumnsNumbered'

prefixAsNamed :: Text -> RowGenColumnHandler Text
prefixAsNamed p = prefixColumns p allColumnsAsNamed

columnSubset :: Ord a => Set a -> RowGenColumnHandler a -> RowGenColumnHandler a
columnSubset s rgch = modifyColumnStateFunction rgch g where
  g f = \x -> if x `Set.member` s then f x else Exclude

renamedHeaderSubset :: Map Text Text -> RowGenColumnHandler Text
renamedHeaderSubset renamedS = GenUsingHeader f where
  f x = case Map.lookup x renamedS of
    Nothing -> Exclude
    Just t -> Include t

namedColumnNumberSubset :: Bool -> Map Int Text -> RowGenColumnHandler Int
namedColumnNumberSubset hasHeader namedS =
  case hasHeader of
    True -> GenIgnoringHeader f
    False -> GenWithoutHeader f
  where
    f n = case Map.lookup n namedS of
      Nothing -> Exclude
      Just t -> Include t

namesGiven :: Bool -> [Text] -> RowGenColumnHandler Int
namesGiven hasHeader names = namedColumnNumberSubset hasHeader m
  where
    m = Map.fromList $ zip [0..] names

-- For ParserOptions
data ParseColumnHandler =
  ParseAll Bool -- ^ True if there's a header and false if not
  | ParseUsingHeader [(Text, ColumnState)]
  | ParseIgnoringHeader [ColumnState]
  | ParseWithoutHeader [ColumnState] deriving (Lift)


colStatesToColNames :: [ColumnState] -> [Text]
colStatesToColNames = catMaybes . fmap f where
  f Exclude = Nothing
  f (Include t) = Just t
{-# INLINEABLE colStatesToColNames #-}


includedColStatesWithHeaders :: [ColumnState] -> [Text] -> [(Text, ColumnState)]
includedColStatesWithHeaders cs hs = catMaybes $ fmap f $ zip hs cs where
  f (_, Exclude) = Nothing
  f x = Just x
{-# INLINEABLE includedColStatesWithHeaders #-}

includedNames :: [ColumnState] -> [Text]
includedNames = catMaybes . fmap f where
  f Exclude = Nothing
  f (Include x) = Just x
{-# INLINEABLE includedNames #-}


colStatesAndHeadersToParseColHandler :: [ColumnState] -> [Text] -> ParseColumnHandler
colStatesAndHeadersToParseColHandler cs hs = ParseUsingHeader $ includedColStatesWithHeaders cs hs
{-# INLINEABLE colStatesAndHeadersToParseColHandler #-}



--ignoreHeaderNamesGiven :: [Text] -> ColumnHandler Text
--ignoreHeaderNamesGiven names =

--newtype HeaderList = HeaderList [Text] deriving (Show, Lift)

--headerListToFilterBools :: [Text] -> HeaderList -> [Bool]
--headerListToFilterBools xs (HeaderList ys) = fmap (`elem` ys) xs
