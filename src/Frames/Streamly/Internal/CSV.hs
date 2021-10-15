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
module Frames.Streamly.Internal.CSV
  (
    -- * Types
    -- ** Text Wrappers
    HeaderText(..)
  , ColTypeName(..)
    -- ** Type-Level representation for Column Id
  , ColumnState(..)
  , ColumnId(..)
  , ColumnIdType
    -- ** For RowGen
  , RowGenColumnSelector(..)
  , OrMissingWhen(..)
  , MissingRequiredIdsF
    -- **  For ParserOptions
  , ParseColumnSelector(..)
  -- * Combinators
  -- ** Building RowGen Column Selectors
  , modifyColumnSelector
  -- ** Building ParserOptions from RowGen and File
  , includedHeaders
  , includedColTypeNames
  , includedColTypeInfo
  , colStatesAndHeadersToParseColHandler
  )
where

import Language.Haskell.TH.Syntax (Lift)

-- | Type to control how column type inference is handled,
-- particularly in the presence of missing data
data OrMissingWhen = NeverMissing -- ^ Infer column type solely from non-missing data,
                   | AlwaysPossible -- ^ Infer column type as @Maybe a@ where @a@ is inferred from non-missing data.
                   | IfSomeMissing -- ^ Infer column type as @a@ if none is missing and @Maybe a@ otherwise.
               deriving (Show, Eq, Lift)


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
data ColumnState = Exclude | Include (ColTypeName, OrMissingWhen) deriving (Eq, Show, Lift)

-- | Type to index column selection and naming behavior
data ColumnId = ColumnByName | ColumnByPosition


-- | Map from 'ColumnId' type to the inhabited type used for
-- column inclusion/exclusion and type-naming.
type family ColumnIdType (a :: ColumnId) :: Type where
  ColumnIdType 'ColumnByName = HeaderText
  ColumnIdType 'ColumnByPosition = Int

type MissingRequiredIdsF (a :: ColumnId) =  [ColumnIdType a] -> [ColumnIdType a]
-- For RowGen

-- |  Type to specify how columns are selected when types are generated
-- by tableTypes.  Types can be generated from header text or column position.
-- This type is parameterized by that choice.
data RowGenColumnSelector (a :: ColumnId) where
  GenUsingHeader :: (HeaderText -> ColumnState) -> MissingRequiredIdsF 'ColumnByName -> RowGenColumnSelector 'ColumnByName
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
includedColTypeNames = mapMaybe f where
  f Exclude = Nothing
  f (Include (x, _)) = Just x
{-# INLINEABLE includedColTypeNames #-}

includedColTypeInfo :: [ColumnState] -> [(ColTypeName, OrMissingWhen)]
includedColTypeInfo = mapMaybe f where
  f Exclude = Nothing
  f (Include x) = Just x
{-# INLINEABLE includedColTypeInfo #-}


colStatesAndHeadersToParseColHandler :: [ColumnState] -> [HeaderText] -> ParseColumnSelector
colStatesAndHeadersToParseColHandler cs hs = ParseUsingHeader $ includedHeaders cs hs
{-# INLINEABLE colStatesAndHeadersToParseColHandler #-}
