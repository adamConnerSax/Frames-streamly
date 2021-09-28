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
