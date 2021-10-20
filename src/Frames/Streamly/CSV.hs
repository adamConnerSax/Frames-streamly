{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveLift #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# LANGUAGE RecordWildCards #-}
{-|
Module      : Frames.Streamly.CSV
Description : CSV parsing/formatting tools for the Frames library, operating via streamly Streams.
Copyright   : (c) Adam Conner-Sax 2020
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

This module can be used in-place of Frames.CSV in order to use streamly streams where Frames uses pipes.
This module adds some functionality for formatting in more flexible ways than the pipes version in Frames.
It allows us of Show instances, in addition to the ShowCSV class included in Frames.  And it allows one-off
specification of a format as well.  See the example for more details.

There is also support for parsing only a subset of columns of a file, which can save time and memory during
loading and parsing of text. See the readme or Frames.Streamly.TH for more details.
-}
module Frames.Streamly.CSV
    (
      -- * Parsing
      ParserOptions(..)
    , defaultParser
    , useRowFilter
      -- * Strict version of ReadRec
    , StrictReadRec (..)
      -- * read from File to Stream of Recs
    , readTable
    , readTableOpt
    , readTableMaybe
    , readTableMaybeOpt
    , readTableEither
    , readTableEitherOpt
      -- * convert streaming Text to streaming Records
    , streamTable
    , streamTableOpt
    , streamTableMaybe
    , streamTableMaybeOpt
    , streamTableEither
    , streamTableEitherOpt
      -- * Produce (streaming) Text from Records
    , streamToCSV
    , streamCSV
    , streamToSV
    , streamSV
    , streamSV'
      -- *  write Records to Text File
    , writeCSV
    , writeSV
    , writeStreamSV
    , writeCSV_Show
    , writeSV_Show
    , writeStreamSV_Show
      -- * Utilities
    , streamToList
    , liftFieldFormatter
    , liftFieldFormatter1
    , formatTextAsIs
    , formatWithShow
    , formatWithShowCSV
    , writeLines
    , writeLines'
    , word8ToTextLines
    -- * TH Support
    , streamTokenized'
    , streamTokenized
    , ColTypeInfo(..)
    , readColHeaders
    -- * debugging
    , streamWord8
    , streamTextLines
    , streamParsed
    , streamParsedMaybe
      -- * Re-exports
    , Separator
    , QuoteChar
    , QuotingMode(..)
    , defaultSep

    )
where

import qualified Frames.Streamly.Internal.CSV as ICSV
import Frames.Streamly.Internal.Streaming (StreamFunctions(..), streamlyFunctions)
import qualified Frames.Streamly.ColumnTypeable as FSCT

import Prelude hiding(getCompose)
import qualified Streamly.Prelude                       as Streamly

import qualified Streamly.Data.Fold                     as Streamly.Fold
#if MIN_VERSION_streamly(0,8,0)
import Streamly.Prelude                       (IsStream)
import qualified Streamly.Internal.Unicode.Array.Char as Streamly.Unicode.Array
import qualified Streamly.Data.Array.Foreign as Streamly.Array
import qualified Streamly.Unicode.Stream           as Streamly.Unicode
#else
import qualified Streamly                               as Streamly
import           Streamly                                ( IsStream )
import qualified Streamly.Internal.Memory.Unicode.Array as Streamly.Unicode.Array
import qualified Streamly.Internal.Memory.Array.Types as Streamly.Array
import qualified Streamly.Data.Unicode.Stream           as Streamly.Unicode
import qualified Streamly.Internal.Data.Fold as Streamly.Fold
#endif

import qualified Streamly.Internal.FileSystem.File      as Streamly.File
import qualified Streamly.Internal.Data.Unfold          as Streamly.Unfold
import           Control.Monad.Catch                     ( MonadThrow(..),MonadCatch )

import Language.Haskell.TH.Syntax (Lift)
import qualified Data.Set as Set

import qualified Data.Strict.Either as Strict
import qualified Data.Text                              as T

import qualified Data.Vinyl                             as Vinyl
import qualified Data.Vinyl.Functor                     as Vinyl
import qualified Data.Vinyl.TypeLevel                   as Vinyl
import qualified Data.Vinyl.Class.Method                as Vinyl

import qualified Frames
import qualified Frames.CSV                             as Frames
import Frames.CSV                             (Separator, QuoteChar, QuotingMode(..), defaultSep)
import qualified Frames.ShowCSV                         as Frames
--import qualified Frames.ColumnTypeable                  as Frames
import qualified Data.Vinyl as V
import qualified Data.Vinyl.Functor as V (Compose(..), (:.))
import GHC.TypeLits (KnownSymbol)



-- | Holds column separator, quote handling and column selection information.
-- NB: This is generated by the 'tableTypes` family of functions
-- in Frames.Streamly.TH. That value should be used if at all possible
-- since it carries the correct column selection info.
data ParserOptions = ParserOptions
  {
    columnSelector :: ICSV.ParseColumnSelector
  , columnSeparator :: Frames.Separator
  , quotingMode :: Frames.QuotingMode
  } deriving Lift

-- | Pull out the parts of 'ParserOptions' required for tokenizing and
-- paste into Frames ParserOptions to pass to Frames tokenizer.
framesParserOptionsForTokenizing :: ParserOptions -> Frames.ParserOptions
framesParserOptionsForTokenizing (ParserOptions _ cs qm) = Frames.ParserOptions Nothing cs qm
{-# INLINE framesParserOptionsForTokenizing #-}

-- | A sensible default *only* for situations when all columns should be parsed.
defaultParser :: ParserOptions
defaultParser = ParserOptions (ICSV.ParseAll True) (Frames.columnSeparator x) (Frames.quotingMode x) where
  x = Frames.defaultParser
{-# INLINE defaultParser #-}

-- | Given a stream of @Records@, for which all fields satisfy the `ShowCSV` constraint,
-- produce a stream of `Text`, one item (line) per `Record` with the specified separator
-- between fields.
streamToSV
  :: forall rs m t.
     ( Frames.ColumnHeaders rs
     , MonadIO m
     , Vinyl.RecordToList rs
     , Vinyl.RecMapMethod Frames.ShowCSV Vinyl.ElField rs
     , IsStream t
     )
  => T.Text -- ^ column separator
  -> t m (Frames.Record rs) -- ^ stream of Records
  -> t m T.Text -- ^ stream of 'Text' rows
streamToSV = streamSVClass @Frames.ShowCSV Frames.showCSV
{-# INLINEABLE streamToSV #-}

-- | Given a stream of @Records@, for which all fields satisfy the `ShowCSV` constraint,
-- produce a stream of CSV `Text`, one item (line) per `Record`.
streamToCSV
  :: forall rs m t
     . ( Frames.ColumnHeaders rs
       , MonadIO m
       , Vinyl.RecordToList rs
       , Vinyl.RecMapMethod Frames.ShowCSV Vinyl.ElField rs
       , IsStream t
       )
  => t m (Frames.Record rs) -- ^ stream of Records
  -> t m T.Text -- ^ stream of 'Text' rows
streamToCSV = streamToSV ","
{-# INLINEABLE streamToCSV #-}

-- | Given a foldable of @Records@, for which all fields satisfy the `ShowCSV` constraint,
-- produce a stream of `Text`, one item (line) per `Record` with the specified separator
-- between fields.
streamSV
  :: forall f rs m t.
     ( Frames.ColumnHeaders rs
     , Foldable f
     , MonadIO m
     , Vinyl.RecordToList rs
     , Vinyl.RecMapMethod Frames.ShowCSV Vinyl.ElField rs
     , IsStream t
     )
  => T.Text -- ^ column separator
  -> f (Frames.Record rs) -- ^ foldable of Records
  -> t m T.Text -- ^ stream of 'Text' rows
streamSV sep = streamToSV sep . Streamly.fromFoldable
{-# INLINEABLE streamSV #-}

-- | Given a foldable of @Records@, for which all fields satisfy the `ShowCSV` constraint,
-- produce a stream of CSV `Text`, one item (line) per `Record`.
streamCSV
  :: forall f rs m t.
     ( Frames.ColumnHeaders rs
     , Foldable f
     , MonadIO m
     , Vinyl.RecordToList rs
     , Vinyl.RecMapMethod Frames.ShowCSV Vinyl.ElField rs
     , IsStream t
     )
  => f (Frames.Record rs)  -- ^ 'Foldable' of Records
  -> t m T.Text -- ^ stream of 'Text' rows
streamCSV = streamSV ","

-- | Convert @Rec@s to lines of `Text` using a class (which must have an instance
-- for each type in the record) to covert each field to `Text`.
streamSVClass
  :: forall c rs t m .
      ( Vinyl.RecMapMethod c Vinyl.ElField rs
      , Vinyl.RecordToList rs
      , Frames.ColumnHeaders rs
      , IsStream t
      , MonadIO m
     )
  => (forall a. c a => a -> T.Text) -- ^ @show@-like function for some constraint satisfied by all fields.
  -> T.Text -- ^ column separator
  -> t m (Frames.Record rs)  -- ^ stream of Records
  -> t m T.Text -- ^ stream of 'Text' rows
streamSVClass toTxt sep s =
  (T.intercalate sep . fmap T.pack $ Frames.columnHeaders (Proxy :: Proxy (Frames.Record rs)))
  `Streamly.cons`
  (Streamly.map (T.intercalate sep . Vinyl.recordToList . Vinyl.rmapMethod @c aux) s)
  where
    aux :: (c (Vinyl.PayloadType Vinyl.ElField a))
        => Vinyl.ElField a
        -> Vinyl.Const T.Text a
    aux (Vinyl.Field x) = Vinyl.Const $ toTxt x


-- | Given a record of functions to map each field to Text,
-- transform a stream of records into a stream of lines of Text,
-- headers first, with headers/fields separated by the given separator.
streamSV'
  :: forall rs t m f.
     (Vinyl.RecordToList rs
     , Vinyl.RApply rs
     , Frames.ColumnHeaders rs
     , IsStream t
     , Streamly.MonadAsync m
     )
  => Vinyl.Rec (Vinyl.Lift (->) f (Vinyl.Const T.Text)) rs -- ^ Vinyl record of formatting functions for the row-type.
  -> T.Text  -- ^ column separator
  -> t m (Frames.Rec f rs)  -- ^ stream of Records
  -> t m T.Text -- ^ stream of 'Text' rows
streamSV' toTextRec sep s =
  (T.intercalate sep . fmap T.pack $ Frames.columnHeaders (Proxy :: Proxy (Frames.Record rs)))
  `Streamly.cons`
  (Streamly.map (T.intercalate sep . Vinyl.recordToList . Vinyl.rapply toTextRec) s)
{-# INLINEABLE streamSV' #-}

-- | Convert a streamly stream into a (lazy) list
streamToList :: (IsStream t, Monad m) => t m a -> m [a]
streamToList = Streamly.toList . Streamly.adapt

-- | lift a field formatting function into the right form to append to a Rec of formatters
liftFieldFormatter :: Vinyl.KnownField t
                   => (Vinyl.Snd t -> T.Text) -- ^ formatting function for the type in Field @t@
                   -> Vinyl.Lift (->) Vinyl.ElField (Vinyl.Const T.Text) t -- ^ formatting function in the form required to use in row-formatters.
liftFieldFormatter toTxt = Vinyl.Lift $ Vinyl.Const . toTxt . Vinyl.getField
{-# INLINEABLE liftFieldFormatter #-}

-- | lift a composed-field formatting function into the right form to append to a Rec of formatters
-- | Perhaps to format a parsed file with @Maybe@ or @Either@ composed with @ElField@
liftFieldFormatter1 :: (Functor f, Vinyl.KnownField t)
                    => (f (Vinyl.Snd t) -> T.Text) -- ^ formatting function for things like @Maybe a@
                    -> Vinyl.Lift (->) (f Vinyl.:. Vinyl.ElField) (Vinyl.Const T.Text) t
liftFieldFormatter1 toTxt = Vinyl.Lift $ Vinyl.Const . toTxt . fmap Vinyl.getField . Vinyl.getCompose
{-# INLINEABLE liftFieldFormatter1 #-}

-- | Format a @Text@ field as-is.
formatTextAsIs :: (Vinyl.KnownField t, Vinyl.Snd t ~ T.Text) => Vinyl.Lift (->) Vinyl.ElField (Vinyl.Const T.Text) t
formatTextAsIs = liftFieldFormatter id
{-# INLINE formatTextAsIs #-}

-- | Format a field using the @Show@ instance of the contained type
formatWithShow :: (Vinyl.KnownField t, Show (Vinyl.Snd t)) => Vinyl.Lift (->) Vinyl.ElField (Vinyl.Const T.Text) t
formatWithShow = liftFieldFormatter $ T.pack . show
{-# INLINE formatWithShow #-}

-- | Format a field using the @Frames.ShowCSV@ instance of the contained type
formatWithShowCSV :: (Vinyl.KnownField t, Frames.ShowCSV (Vinyl.Snd t)) => Vinyl.Lift (->) Vinyl.ElField (Vinyl.Const T.Text) t
formatWithShowCSV = liftFieldFormatter Frames.showCSV
{-# INLINE formatWithShowCSV #-}

-- NB: Uses some internal modules from Streamly.  Will have to change when they become stable
-- | write a stream of @Text@ to a file, one line per stream item.
writeLines' :: (Streamly.MonadAsync m, MonadCatch m, Streamly.IsStream t) => FilePath -> t m T.Text -> m ()
writeLines' fp s = do
#if MIN_VERSION_streamly(0,8,0)
  let unfoldMany = Streamly.unfoldMany
#else
  let unfoldMany = Streamly.concatUnfold
#endif
  Streamly.fold (Streamly.File.write fp)
    $ Streamly.Unicode.encodeUtf8
    $ Streamly.adapt
    $ unfoldMany Streamly.Unfold.fromList
    $ Streamly.map T.unpack
    $ Streamly.intersperse "\n" s
{-# INLINEABLE writeLines' #-}

-- | write a stream of @Text@ to a file, one line per stream item.
-- | Monomorphised to serial streams for ease of use.
writeLines :: (Streamly.MonadAsync m, MonadCatch m) => FilePath -> Streamly.SerialT m T.Text -> m ()
writeLines = writeLines'
{-# INLINE writeLines #-}

-- NB: Uses some internal modules from Streamly.  Will have to change when they become stable
-- | write a stream of @Records@ to a file, one line per @Record@.
-- Use the 'Frames.ShowCSV' class to format each field to @Text@
writeStreamSV
  ::  forall rs m t.
   ( Frames.ColumnHeaders rs
   , MonadCatch m
   , Vinyl.RecordToList rs
   , Vinyl.RecMapMethod Frames.ShowCSV Vinyl.ElField rs
   , IsStream t
   , Streamly.MonadAsync m
   )
  => T.Text -- ^ column separator
  -> FilePath -- ^ path
  -> t m (Frames.Record rs) -- ^ stream of Records
  -> m ()
writeStreamSV sep fp = writeLines' fp . streamToSV sep
{-# INLINEABLE writeStreamSV #-}

-- | write a foldable of @Records@ to a file, one line per @Record@.
-- Use the 'Frames.ShowCSV' class to format each field to @Text@
writeSV
  ::  forall rs m f.
   ( Frames.ColumnHeaders rs
   , MonadCatch m
   , Vinyl.RecordToList rs
   , Vinyl.RecMapMethod Frames.ShowCSV Vinyl.ElField rs
   , Streamly.MonadAsync m
   , Foldable f
   )
  => T.Text -- ^ column separator
  -> FilePath -- ^ file path
  -> f (Frames.Record rs) -- ^ Foldable of Records
  -> m ()
writeSV sep fp = writeStreamSV sep fp . Streamly.fromFoldable @Streamly.AheadT
{-# INLINEABLE writeSV #-}

-- | write a foldable of @Records@ to a file, one line per @Record@.
-- Use the 'Frames.ShowCSV' class to format each field to @Text@
writeCSV
  ::  forall rs m f.
   ( Frames.ColumnHeaders rs
   , MonadCatch m
   , Vinyl.RecordToList rs
   , Vinyl.RecMapMethod Frames.ShowCSV Vinyl.ElField rs
   , Streamly.MonadAsync m
   , Foldable f
   )
  => FilePath -- ^ file path
  -> f (Frames.Record rs) -- ^ 'Foldable' of Records
  -> m ()
writeCSV fp = writeSV "," fp
{-# INLINEABLE writeCSV #-}

-- NB: Uses some internal modules from Streamly.  Will have to change when they become stable

-- | write a stream of @Records@ to a file, one line per @Record@.
-- Use the 'Show' class to format each field to @Text@
writeStreamSV_Show
  ::  forall rs m t.
   ( Frames.ColumnHeaders rs
   , MonadCatch m
   , Vinyl.RecordToList rs
   , Vinyl.RecMapMethod Show Vinyl.ElField rs
   , IsStream t
   , Streamly.MonadAsync m
   )
  => T.Text -- ^ column separator
  -> FilePath -- ^ file path
  -> t m (Frames.Record rs) -- ^ stream of Records
  -> m ()
writeStreamSV_Show sep fp = writeLines' fp . streamSVClass @Show (T.pack . show) sep
{-# INLINEABLE writeStreamSV_Show #-}

-- | write a foldable of @Records@ to a file, one line per @Record@.
-- Use the 'Show' class to format each field to @Text@
writeSV_Show
  ::  forall rs m f.
   ( Frames.ColumnHeaders rs
   , MonadCatch m
   , Vinyl.RecordToList rs
   , Vinyl.RecMapMethod Show Vinyl.ElField rs
   , Streamly.MonadAsync m
   , Foldable f
   )
  => T.Text -- ^ column separator
  -> FilePath  -- ^ file path
  -> f (Frames.Record rs) -- ^ 'Foldable' of Records
  -> m ()
writeSV_Show sep fp = writeStreamSV_Show sep fp . Streamly.fromFoldable @Streamly.AheadT
{-# INLINEABLE writeSV_Show #-}

-- | write a foldable of @Records@ to a file, one line per @Record@.
-- Use the 'Show' class to format each field to @Text@
writeCSV_Show
  ::  forall rs m f.
   ( Frames.ColumnHeaders rs
   , MonadCatch m
   , Vinyl.RecordToList rs
   , Vinyl.RecMapMethod Show Vinyl.ElField rs
   , Streamly.MonadAsync m
   , Foldable f
   )
  => FilePath -- ^ file path
  -> f (Frames.Record rs) -- ^ 'Foldable' of Records
  -> m ()
writeCSV_Show fp = writeSV_Show "," fp
{-# INLINEABLE writeCSV_Show #-}

-- Thanks to Tim Pierson for the functions below!

-- | Stream a table from a file path, using the default options.
-- Results composed with the @Maybe@ functor. Unparsed fields are returned as @Nothing@.
-- NB:  If the inferred/given rs is different from the actual file row-type, things will go awry.
readTableMaybe
    :: forall rs t m.
    (Streamly.MonadAsync m
    , MonadCatch m
    , IsStream t
    , Monad (t m)
    , Vinyl.RMap rs
    , StrictReadRec rs)
    => FilePath -- ^ file path
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs) -- ^ stream of @Maybe :. ElField@ records after parsing.
readTableMaybe = readTableMaybeOpt defaultParser
{-# INLINEABLE readTableMaybe #-}

-- | Stream a table from a file path.
-- Results composed with the @Maybe@ functor. Unparsed fields are returned as @Nothing@.
-- NB:  If the inferred/given rs is different from the actual file row-type, things will go awry.
readTableMaybeOpt
    :: forall rs t m.
    (Streamly.MonadAsync m
    , MonadCatch m
    , IsStream t
    , Monad (t m)
    , Vinyl.RMap rs
    , StrictReadRec rs)
    => ParserOptions -- ^ parsing options
    -> FilePath -- ^ file path
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs) -- ^ stream of @Maybe :. ElField@ records after parsing.
readTableMaybeOpt opts = Streamly.map recEitherToMaybe . readTableEitherOpt opts
{-# INLINEABLE readTableMaybeOpt #-}

-- | Stream a table from a file path.
-- Results composed with the @Either Text@ functor. Unparsed fields are returned as a @Left@
-- containing the string that failed to parse.
-- Uses default options.
-- NB:  If the inferred/given rs is different from the actual file row-type, things will go awry.
readTableEither
  :: forall rs t m.
     (Streamly.MonadAsync m
     , MonadCatch m
     , IsStream t
     , Monad (t m)
     , Vinyl.RMap rs
     , StrictReadRec rs)
  => FilePath -- ^ file path
  -> t m (Vinyl.Rec (Either T.Text Vinyl.:. Vinyl.ElField) rs) -- ^ stream of @Either :. ElField@ records after parsing.
readTableEither = readTableEitherOpt defaultParser
{-# INLINEABLE readTableEither #-}

-- | Stream a table from a file path.
-- Results composed with the @Either Text@ functor. Unparsed fields are returned as a @Left@
-- containing the string that failed to parse.
-- NB:  If the inferred/given rs is different from the actual file row-type, things will go awry.
readTableEitherOpt
  :: forall rs t m.
     (Streamly.MonadAsync m
     , MonadCatch m
     , IsStream t
     , Monad (t m)
     , Vinyl.RMap rs
     , StrictReadRec rs)
  => ParserOptions -- ^ parsing options
  -> FilePath -- ^ file path
  -> t m (Vinyl.Rec (Either T.Text Vinyl.:. Vinyl.ElField) rs) -- ^ stream of @Either :. ElField@ records after parsing.
readTableEitherOpt opts = streamTableEitherOpt opts . word8ToTextLines . Streamly.File.toBytes
{-# INLINEABLE readTableEitherOpt #-}

-- | Stream Table from a file path, dropping rows where any field fails to parse
-- | Use default options
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
readTable
  :: forall rs t m.
     (Streamly.MonadAsync m
     , MonadCatch m
     , IsStream t
     , Monad (t m)
     , Vinyl.RMap rs
     , StrictReadRec rs)
  => FilePath -- ^ file path
  -> t m (Frames.Record rs) -- ^ stream of Records
readTable = readTableOpt defaultParser
{-# INLINEABLE readTable #-}

-- | Stream Table from a file path, dropping rows where any field fails to parse
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
readTableOpt
  :: forall rs t m.
     (Streamly.MonadAsync m
     , MonadCatch m
     , IsStream t
     , Monad (t m)
     , Vinyl.RMap rs
     , StrictReadRec rs)
  => ParserOptions  -- ^ parsing options
  -> FilePath -- ^ file path
  -> t m (Frames.Record rs)  -- ^ stream of Records
readTableOpt !opts !fp = streamTableOpt opts $! word8ToTextLines $! Streamly.File.toBytes fp
{-# INLINEABLE readTableOpt #-}

-- | Convert a stream of lines of `Text` to a table
-- Each field is returned in an @Either Text@ functor. @Right a@ for successful parses
-- and @Left Text@ when parsing fails, containing the text that failed to Parse.
--
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
streamTableEither
    :: forall rs t m.
    (Streamly.MonadAsync m
    , IsStream t
    , Monad (t m)
    , Vinyl.RMap rs
    , StrictReadRec rs)
    => Streamly.SerialT m T.Text -- ^ stream of 'Text' rows
    -> t m (Vinyl.Rec (Either T.Text Vinyl.:. Vinyl.ElField) rs) -- ^ stream of parsed @Either :. ElField@ rows
streamTableEither = streamTableEitherOpt defaultParser
{-# INLINEABLE streamTableEither #-}


-- | Various parsing options have to handle the header line, if it exists,
-- differently.  This function pulls all that logic into one place.
-- We take the 'ParserOptions' and the stream of un-tokenized 'Text' lines
-- and do whatever is required, checking for various errors
-- (empty stream, missing headers, wrong number of columns when using positions for typing/naming)
-- along the way.
handleHeader :: forall s m.
                StreamFunctions s m
             -> ParserOptions
             -> s m T.Text
             -> m (Maybe [Bool], s m T.Text)
handleHeader StreamFunctions{..} opts s = case columnSelector opts of
  ICSV.ParseAll True -> return (Nothing, dropFirst s)
  ICSV.ParseAll False ->  return (Nothing, s)
  ICSV.ParseIgnoringHeader cs -> checkNumFirstRowCols s cs >> return (Just $ csToBool <$> cs, dropFirst s)
  ICSV.ParseWithoutHeader cs -> checkNumFirstRowCols s cs >> return (Just $ csToBool <$> cs, s)
  ICSV.ParseUsingHeader hs -> (, dropFirst s) . Just <$> boolsFromHeader hs s
  where
    dropFirst = sDrop 1
    csToBool x = if x == ICSV.Exclude then False else True

    tokenizedFirstRow :: s m T.Text -> m [Text]
    tokenizedFirstRow s' = do
      mht <- sUncons s'
      case mht of
        Nothing -> throwM ICSV.EmptyStreamException
        Just (x, _) -> return . Frames.tokenizeRow (framesParserOptionsForTokenizing opts)

    boolsFromHeader :: [ICSV.HeaderText] ->  s m T.Text -> m [Bool]
    boolsFromHeader hs s' = do
      let headersToInclude = Set.fromList hs
          includeHeader t = t `Set.member` headersToInclude
      fileHeaders <- ICSV.HeaderText <<$>> tokenizedFirstRow s'
      let fileHeadersS = Set.fromList fileHeaders
          notPresentM t = if t `Set.member` fileHeadersS then Nothing else Just t
          missingIncluded = catMaybes $ fmap notPresentM hs
      unless (null missingIncluded) $ throwM $ ICSV.MissingHeadersException missingIncluded
      let bools = includeHeader <$> fileHeaders
      return bools

    checkNumFirstRowCols :: s m T.Text -> [ICSV.ColumnState] -> m ()
    checkNumFirstRowCols s' cs = tokenizedFirstRow s' >>= checkSameLength cs

    checkSameLength :: [ICSV.ColumnState] -> [b] -> m ()
    checkSameLength givenCSs streamCols = do
      let nGiven = length givenCSs
          nStreamCols = length streamCols
          errMsg = "Number of given columns from type generation (" <> show nGiven
                   <> ") doesn't match the number of columns in the parsed file ("
                   <> show nStreamCols <> ")"
      when (nGiven /= nStreamCols) $ throwM $ ICSV.WrongNumberColumnsException errMsg
      return ()

-- | Convert a stream of lines of `Text` to records.
-- Each field is returned in an @Either Text@ functor. @Right a@ for successful parses
-- and @Left Text@ when parsing fails, containing the text that failed to Parse.
--
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will...go awry.
streamTableEitherOpt
    :: forall rs t m.
    (Streamly.MonadAsync m
    , IsStream t
    , Monad (t m)
    , Vinyl.RMap rs
    , StrictReadRec rs)
    => ParserOptions -- ^ parsing options
    -> Streamly.SerialT m T.Text -- ^ stream of 'Text' rows
    -> t m (Vinyl.Rec ((Either T.Text) Vinyl.:. Vinyl.ElField) rs)  -- ^ stream of parsed @Either :. ElField@ rows
streamTableEitherOpt opts s = do
  (rF, s') <- fromEffect $ handleHeader opts s
  Streamly.map (recUnStrictEither . parse . useRowFilter rF . Frames.tokenizeRow (framesParserOptionsForTokenizing opts)) s'
  where
    parse = strictReadRec
{-# INLINEABLE streamTableEitherOpt #-}

-- | Convert a stream of lines of `Text` to a table.
--
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will..go awry.
streamTableMaybe
    :: forall rs t m.
    (Streamly.MonadAsync m
    , IsStream t
    , Monad (t m)
    , Vinyl.RMap rs
    , StrictReadRec rs)
    => Streamly.SerialT m T.Text -- ^ stream of 'Text' rows
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs) -- ^ stream of parsed @Maybe :. ElField@ rows
streamTableMaybe = streamTableMaybeOpt defaultParser
{-# INLINEABLE streamTableMaybe #-}

-- | Convert a stream of lines of Text to a table .
--
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will..go awry.
streamTableMaybeOpt
    :: forall rs t m.
    (Streamly.MonadAsync m
    , IsStream t
    , Monad (t m)
    , Vinyl.RMap rs
    , StrictReadRec rs)
    => ParserOptions -- ^ parsing options
    -> Streamly.SerialT m T.Text -- ^ stream of 'Text' rows
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs) -- ^ stream of parsed @Maybe :. ElField@ rows
streamTableMaybeOpt opts = Streamly.map recEitherToMaybe . streamTableEitherOpt opts
{-# INLINEABLE streamTableMaybeOpt #-}

-- | Convert a stream of lines of 'Text' to a table,
-- dropping rows where any field fails to parse.
-- Use default options.
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
streamTable
    :: forall rs t m.
    (Streamly.MonadAsync m
    , IsStream t
    , Monad (t m)
    , Vinyl.RMap rs
    , StrictReadRec rs
    )
    => Streamly.SerialT m T.Text -- ^ stream of 'Text' rows
    -> t m (Frames.Record rs) -- ^ stream of Records
streamTable = streamTableOpt defaultParser
{-# INLINEABLE streamTable #-}

fromEffect :: (Monad m, IsStream t) => m a -> t m a
#if MIN_VERSION_streamly(0,8,0)
fromEffect = Streamly.fromEffect
#else
fromEffect = Streamly.yieldM
#endif
{-# INLINE fromEffect #-}

-- | Convert a stream of lines of 'Text' `Word8` to a table,
-- dropping rows where any field fails to parse.
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
streamTableOpt
    :: forall rs t m.
    (Streamly.MonadAsync m
    , IsStream t
    , Monad (t m)
    , Vinyl.RMap rs
    , StrictReadRec rs
    )
    => StreamFunctions s m
    -> ParserOptions -- ^ parsing options
    -> s m T.Text  -- ^ stream of 'Text' rows
    -> s m (Frames.Record rs) -- ^ stream of Records
streamTableOpt StreamFunctions{..} opts s = do
  (rF, s') <- fromEffect $ handleHeader opts s
  Streamly.mapMaybe (mRec rF) s'
  where
    mRec rf x = Frames.recMaybe $! doParseStrict $! useRowFilter rf $! Frames.tokenizeRow (framesParserOptionsForTokenizing opts) x
{-# INLINEABLE streamTableOpt #-}

-- | Parse using StrictReadRec
doParseStrict :: (V.RMap rs, StrictReadRec rs) => [Text] -> V.Rec (Maybe V.:. V.ElField) rs
doParseStrict !x = recStrictEitherToMaybe $! strictReadRec x
{-# INLINEABLE doParseStrict #-}

recEitherToMaybe :: Vinyl.RMap rs => Vinyl.Rec (Either T.Text Vinyl.:. Vinyl.ElField) rs -> Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs
recEitherToMaybe = Vinyl.rmap (either (const (Vinyl.Compose Nothing)) (Vinyl.Compose . Just) . Vinyl.getCompose)
{-# INLINE recEitherToMaybe #-}

recStrictEitherToMaybe :: Vinyl.RMap rs => Vinyl.Rec (Strict.Either T.Text Vinyl.:. Vinyl.ElField) rs -> Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs
recStrictEitherToMaybe = Vinyl.rmap (Strict.either (const (Vinyl.Compose Nothing)) (Vinyl.Compose . Just) . Vinyl.getCompose)
{-# INLINE recStrictEitherToMaybe #-}


recUnStrictEither :: Vinyl.RMap rs => Vinyl.Rec (Strict.Either T.Text Vinyl.:. Vinyl.ElField) rs -> Vinyl.Rec (Either T.Text Vinyl.:. Vinyl.ElField) rs
recUnStrictEither = Vinyl.rmap (Vinyl.Compose . Strict.either Left Right . Vinyl.getCompose)
{-# INLINE recUnStrictEither #-}
{-
-- | Convert a stream of Word8 to lines of `Text` by decoding as UTF8 and splitting on "\n"
word8ToTextLines :: (IsStream t, MonadIO m) => t m Word8 -> t m T.Text
word8ToTextLines =  Streamly.splitOnSuffix(=='\n') (toText <$> Streamly.Fold.toList)
                    . Streamly.Unicode.decodeUtf8
{-# INLINE word8ToTextLines #-}

word8ToTextLines2 :: (IsStream t, MonadIO m) => t m Word8 -> t m T.Text
word8ToTextLines2 =  Streamly.map (toText . Streamly.Array.toList)
                     . Streamly.Unicode.Array.lines
                     . Streamly.Unicode.decodeUtf8
{-# INLINE word8ToTextLines2 #-}
-}
-- | Parse list of text into @Rec (Either :. ElField)@ using strict @Either@
class StrictReadRec rs where
  strictReadRec :: [Text] -> V.Rec (Strict.Either Text V.:. V.ElField) rs

instance StrictReadRec '[] where
  strictReadRec _ = V.RNil
  {-# INLINEABLE strictReadRec #-}

instance (FSCT.Parseable t, StrictReadRec ts, KnownSymbol s) => StrictReadRec (s Frames.:-> t ': ts) where
  -- This one is neccesary to avoid a warning about incomplete pattern matches. ??
  strictReadRec [] = V.Compose (Strict.Left mempty) V.:& strictReadRec []

  strictReadRec (!h : t) = maybe
                           (V.Compose (Strict.Left (T.copy h)))
                           (V.Compose . Strict.Right . V.Field)
                           (FSCT.parse' h)
                           V.:& strictReadRec t
  {-# INLINEABLE strictReadRec #-}

-- TH Support
useRowFilter :: Maybe [Bool] -> [Text] -> [Text]
useRowFilter = maybe id f  where
  f bs xs = snd <$> filter fst (zip bs xs)
{-# INLINEABLE useRowFilter #-}

prefixInference :: forall a s m.(MonadThrow m
                                , Monad m
                                , FSCT.ColumnTypeable a
                                )
                => StreamFunctions s m
                -> FSCT.Parsers a
                -> (Text -> Bool)
                -> Maybe [Bool]
                -> s m [T.Text]
                -> m [a]
prefixInference StreamFunctions{..} parsers isMissing rF s = do
  sThrowIfEmpty s
  let inferCols = fmap (FSCT.inferType @a parsers isMissing) -- need type application here to know what col-type to infer
      step ts = zipWith (FSCT.updateWithParse parsers) ts . inferCols
      start = repeat FSCT.initialColType

  sFolder step start $ sMapper (useRowFilter rF) s
{-# INLINEABLE prefixInference #-}

data ColTypeInfo a = ColTypeInfo { colTypeName :: ICSV.ColTypeName, orMissingWhen :: ICSV.OrMissingWhen, colBaseType :: a}

-- | Extract column names and inferred types from a CSV file.
readColHeaders :: forall a b s m.
                  (Show (ICSV.ColumnIdType b)
                  , Monad m
                  , MonadThrow m
                  , FSCT.ColumnTypeable a
                  )
               => StreamFunctions s m
               -> FSCT.Parsers a
               -> ICSV.RowGenColumnSelector b-- headerOverride
               -> s m [Text]
               -> m ([ColTypeInfo a], ICSV.ParseColumnSelector)
readColHeaders sf@StreamFunctions{..} parsers rgColHandler s =  do
  let csToBool =  (/= ICSV.Exclude)
  -- headerRow :: [(ICSV.ColTypeName , ICSV.OrMissingWhen)]
  -- pch :: ICSV.ParseColumnSelector
  -- rF :: Maybe [Bool]
  mUncons <- sUncons s-- @Streamly.SerialT s
  (firstRow, mTail) <- maybe (throwM ICSV.EmptyStreamException) return mUncons
  (headerRow, pch, rF, inferS) <- case rgColHandler of
    ICSV.GenUsingHeader f mrF -> do
      let allHeaders = ICSV.HeaderText <$> firstRow -- (draw >>= maybe err return)
      checkColumnIds mrF allHeaders
      let allColStates = f <$> allHeaders
          allBools = csToBool <$> allColStates
          includedInfo = ICSV.includedColTypeInfo allColStates
          parseColHeader = ICSV.colStatesAndHeadersToParseColHandler allColStates allHeaders
      return (includedInfo, parseColHeader, Just allBools, mTail)
    ICSV.GenIgnoringHeader f mrF -> do
      let allHeaders = firstRow -- <- draw >>= maybe err return
          allIndexes = [0..(length allHeaders - 1)]
      checkColumnIds mrF allIndexes
      let allColStates = f  <$> allIndexes
          allBools = csToBool <$> allColStates
          includedInfo = ICSV.includedColTypeInfo allColStates
          parseColHeader = ICSV.ParseIgnoringHeader allColStates
      return (includedInfo, parseColHeader, Just allBools, mTail)
    ICSV.GenWithoutHeader f mrF -> do
      let sampleRow = firstRow -- <- peek >>= maybe err return
          allIndexes = [0..(length sampleRow - 1)]
      checkColumnIds mrF allIndexes
      let allColStates =  f <$> allIndexes
          allBools = csToBool <$> allColStates
          includedInfo = ICSV.includedColTypeInfo allColStates
          parseColHeader = ICSV.ParseWithoutHeader allColStates
      return (includedInfo, parseColHeader, Just allBools, s)
  let isMissing t = T.null t || t == "NA"
      assembleCTI :: (ICSV.ColTypeName, ICSV.OrMissingWhen) -> a -> ColTypeInfo a
      assembleCTI (a, b) c = ColTypeInfo a b c
  colTypes <- prefixInference sf parsers isMissing rF inferS
  unless (length headerRow == length colTypes) $ errNumColumns headerRow colTypes
  return (zipWith assembleCTI headerRow colTypes, pch)
  where errNumColumns hs cts =
          throwM
          $ ICSV.BadHeaderException
          $ (unlines
          [ ""
          , "Error parsing CSV: "
          , "  Number of columns in header differs from number of columns"
          , "  found in the remaining file. This may be due to newlines"
          , "  being present within the data itself (not just separating"
          , "  rows). If support for embedded newlines is required, "
          , "  consider using the Frames-dsv package in conjunction with"
          , "  Frames to make use of a different CSV parser."])
          <> "\nHeaders: " <> show hs <> "\nNumber ColTypes: " <> show (length cts)
        checkColumnIds :: ICSV.MissingRequiredIdsF b -> [ICSV.ColumnIdType b] -> m ()
        checkColumnIds mrF fileIds = do
          let missing = mrF fileIds
          unless (null missing)
            $ throwM
            $ ICSV.BadHeaderException
            $ "Required columnIds (headers or positions) are missing from file being parsed.  Missing=" <> show missing
          return ()
{-# INLINEABLE readColHeaders #-}
{-
streamWord8 :: (Streamly.IsStream t, Streamly.MonadAsync m, MonadCatch m) => FilePath -> t m Word8
streamWord8 =  Streamly.File.toBytes
{-# INLINE streamWord8 #-}

streamTextLines :: (Streamly.IsStream t, Streamly.MonadAsync m, MonadCatch m) => FilePath -> t m Text
streamTextLines = word8ToTextLines2 . streamWord8
{-# INLINE streamTextLines #-}
-}

streamTokenized' :: StreamFunctions s m -> FilePath -> Frames.Separator -> s m [Text]
streamTokenized' StreamFunctions{..} fp sep =  sMapper (fmap T.copy . Frames.tokenizeRow popts) $ sTextLines fp where
  popts = Frames.defaultParser { Frames.columnSeparator = sep }
{-# INLINE streamTokenized' #-}

streamTokenized :: StreamFunctions s m -> FilePath -> s m [Text]
streamTokenized StreamFunctions{..} =  sMapper (fmap T.copy . Frames.tokenizeRow Frames.defaultParser) . sTextLines
{-# INLINE streamTokenized #-}

streamParsed :: (V.RMap rs, StrictReadRec rs) => StreamFunctions s m -> FilePath -> s m (V.Rec (Strict.Either Text V.:. V.ElField) rs)
streamParsed StreamFunctions{..} = sMapper (strictReadRec . Frames.tokenizeRow Frames.defaultParser) . sTextLines
{-# INLINE streamParsed #-}

streamParsedMaybe :: (V.RMap rs, StrictReadRec rs) => StreamFunctions s m-> FilePath -> s m (V.Rec (Maybe V.:. V.ElField) rs)
streamParsedMaybe StreamFunctions{..} =  sMapper (recStrictEitherToMaybe . strictReadRec . Frames.tokenizeRow Frames.defaultParser) . sTextLines
{-# INLINE streamParsedMaybe #-}
