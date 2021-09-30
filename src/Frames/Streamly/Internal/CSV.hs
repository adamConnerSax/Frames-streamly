{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveLift #-}
{-# LANGUAGE FlexibleContexts #-}
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

-- | Wrapper for the text value of the column header to match on in column selection
newtype HeaderText = HeaderText { headerText :: Text } deriving (Show, Eq, Ord, Lift)

-- | Wrapper for the text of the type name we are generating for a column.
-- NB: This might be almost the same as 'HeaderText' (up to capitalization, etc.)
-- But it might be different:
-- 1. Due to prefixing or renaming.
-- 2. If we ignore the header or don't have one, then this name comes from the
-- column position rather than the header text.
newtype ColTypeName = ColTypeName { colTypeName :: Text } deriving (Show, Eq, Ord, Lift)

-- | Per-column indicator of exclusion or type-name to generate when included.
-- Isomorphic to @Maybe ColTypeName@ but clearer in use.
data ColumnState = Exclude | Include ColTypeName deriving (Eq, Show,Lift)

-- | Type to index column selection and naming behavior
data ColumnId = ColumnByName | ColumnByPosition


-- | Map from 'ColumnId' type to the inhabited type used for
-- column inclusion/exclusion and type-naming.
type family ColumnIdType (a :: ColumnId) :: Type where
  ColumnIdType 'ColumnByName = HeaderText
  ColumnIdType 'ColumnByPosition = Int
{-
data RequiredColumnIds (a :: ColumnId) where
  NoRequiredColumnIds :: RequiredColumnIds a
  RequiredColumnIds :: Ord (ColumnIdType a) => Set (ColumnIdType a) -> RequiredColumnIds a
-}
type MissingRequiredIdsF (a :: ColumnId) =  [ColumnIdType a] -> [ColumnIdType a]
-- For RowGen

-- |  Type to specify how columns are selected when types are generated
-- by tableTypes.  Types can be generated from header text or column position.
-- This type is parameterized by that choice.
data RowGenColumnSelector (a :: ColumnId) where
  GenUsingHeader :: (HeaderText -> ColumnState)-> MissingRequiredIdsF 'ColumnByName -> RowGenColumnSelector 'ColumnByName
  GenIgnoringHeader :: (Int -> ColumnState) -> MissingRequiredIdsF 'ColumnByPosition  -> RowGenColumnSelector 'ColumnByPosition
  GenWithoutHeader ::  (Int -> ColumnState) ->  MissingRequiredIdsF 'ColumnByPosition -> RowGenColumnSelector 'ColumnByPosition

-- | combinator to update or switch out the column selection function of a RowGenColumnSelector
modifyColumnSelector :: RowGenColumnSelector a
                     -> ((ColumnIdType a -> ColumnState) -> (ColumnIdType a -> ColumnState))
                     -> (MissingRequiredIdsF a -> MissingRequiredIdsF a)
                     -> RowGenColumnSelector a
modifyColumnSelector (GenUsingHeader f mr) g h = GenUsingHeader (g f) (h mr)
modifyColumnSelector (GenIgnoringHeader f mr) g h = GenIgnoringHeader (g f) (h mr)
modifyColumnSelector (GenWithoutHeader f mr) g h = GenWithoutHeader (g f) (h mr)


-- | Type to control how the csv parsers include/exclude columns
data ParseColumnSelector =
  ParseAll Bool -- ^ True if there's a header and false if not
  | ParseUsingHeader [HeaderText]
  | ParseIgnoringHeader [ColumnState]
  | ParseWithoutHeader [ColumnState] deriving (Lift, Show)

-- Helpers for generating the Correct ParseColumnSelector
includedHeaders :: [ColumnState] -> [HeaderText] -> [HeaderText]
includedHeaders cs hs = catMaybes $ fmap f $ zip hs cs where
  f (_, Exclude) = Nothing
  f (x, Include _) = Just x
{-# INLINEABLE includedHeaders #-}

includedColTypeNames :: [ColumnState] -> [ColTypeName]
includedColTypeNames = catMaybes . fmap f where
  f Exclude = Nothing
  f (Include x) = Just x
{-# INLINEABLE includedColTypeNames #-}


colStatesAndHeadersToParseColHandler :: [ColumnState] -> [HeaderText] -> ParseColumnSelector
colStatesAndHeadersToParseColHandler cs hs = ParseUsingHeader $ includedHeaders cs hs
{-# INLINEABLE colStatesAndHeadersToParseColHandler #-}
