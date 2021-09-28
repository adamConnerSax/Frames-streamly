{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveLift #-}
{-# Language GADTs #-}
{-# LANGUAGE TypeFamilies #-}
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

data ColumnId = ColumnByName | ColumnByPosition

type family ColumnIdType (a :: ColumnId) :: Type where
  ColumnIdType 'ColumnByName = Text
  ColumnIdType 'ColumnByPosition = Int

-- For RowGen

-- |  Type to specify how columns are selected when types are generated
-- by tableTypes.  Types can be generated from header text or column position.
-- This type is parameterized by that choice.
data RowGenColumnSelector (a :: ColumnId) where
  GenUsingHeader :: (Text -> ColumnState) -> RowGenColumnSelector 'ColumnByName
  GenIgnoringHeader :: (Int -> ColumnState) -> RowGenColumnSelector 'ColumnByPosition
  GenWithoutHeader :: (Int -> ColumnState) -> RowGenColumnSelector 'ColumnByPosition

-- | combinator to update or switch out the column selection function of a RowGenColumnSelector
modifyColumnSelector :: RowGenColumnSelector a
                     -> ((ColumnIdType a -> ColumnState) -> (ColumnIdType a -> ColumnState))
                     -> RowGenColumnSelector a
modifyColumnSelector (GenUsingHeader f) g = GenUsingHeader $ g f
modifyColumnSelector (GenIgnoringHeader f) g = GenIgnoringHeader $ g f
modifyColumnSelector (GenWithoutHeader f) g = GenWithoutHeader $ g f


-- For ParserOptions
data ParseColumnSelector =
  ParseAll Bool -- ^ True if there's a header and false if not
  | ParseUsingHeader [(Text, ColumnState)]
  | ParseIgnoringHeader [ColumnState]
  | ParseWithoutHeader [ColumnState] deriving (Lift)

-- Helpers for generating the Correct ParseColumnSelector

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


colStatesAndHeadersToParseColHandler :: [ColumnState] -> [Text] -> ParseColumnSelector
colStatesAndHeadersToParseColHandler cs hs = ParseUsingHeader $ includedColStatesWithHeaders cs hs
{-# INLINEABLE colStatesAndHeadersToParseColHandler #-}



--ignoreHeaderNamesGiven :: [Text] -> ColumnHandler Text
--ignoreHeaderNamesGiven names =

--newtype HeaderList = HeaderList [Text] deriving (Show, Lift)

--headerListToFilterBools :: [Text] -> HeaderList -> [Bool]
--headerListToFilterBools xs (HeaderList ys) = fmap (`elem` ys) xs
