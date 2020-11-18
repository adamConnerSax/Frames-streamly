{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}
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
-}
module Frames.Streamly.CSV
    (
      -- * read from File to Stream of Recs 
      readTable
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
    )
where

import qualified Streamly.Prelude                       as Streamly
import qualified Streamly                               as Streamly
import           Streamly                                ( IsStream )
import qualified Streamly.Data.Fold                     as Streamly.Fold
import qualified Streamly.Data.Unicode.Stream           as Streamly.Unicode
import qualified Streamly.Internal.FileSystem.File      as Streamly.File
import qualified Streamly.Internal.Data.Unfold          as Streamly.Unfold
import           Control.Monad.Catch                     ( MonadCatch )
import           Control.Monad.IO.Class                  ( MonadIO )

import           Data.Maybe                              (isNothing)
import qualified Data.Text                              as T

import qualified Data.Vinyl                             as Vinyl
import qualified Data.Vinyl.Functor                     as Vinyl
import qualified Data.Vinyl.TypeLevel                   as Vinyl
import qualified Data.Vinyl.Class.Method                as Vinyl
import           Data.Word                               ( Word8 )

import qualified Frames                                 as Frames
import qualified Frames.CSV                             as Frames
import qualified Frames.ShowCSV                         as Frames 

import Data.Proxy (Proxy (..))

  

-- | Given a stream of @Records@, for which all fields satisfy the `ShowCSV` constraint,
-- produce a stream of `Text`, one item (line) per `Record` with the specified separator
-- between fields.
streamToSV
  :: forall rs m t.
     ( Frames.ColumnHeaders rs
     , Monad m
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
       , Monad m
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
     , Monad m
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
     , Monad m
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
      , Monad m
     )
  => (forall a. c a => a -> T.Text) -- ^ @show@-like function for some constraint satisfied by all fields.
  -> T.Text -- ^ column separator
  -> t m (Frames.Record rs)  -- ^ stream of Records
  -> t m T.Text -- ^ stream of 'Text' rows
streamSVClass toText sep s =
  (T.intercalate sep . fmap T.pack $ Frames.columnHeaders (Proxy :: Proxy (Frames.Record rs)))
  `Streamly.cons`
  (Streamly.map (T.intercalate sep . Vinyl.recordToList . Vinyl.rmapMethod @c aux) s)
  where
    aux :: (c (Vinyl.PayloadType Vinyl.ElField a))
        => Vinyl.ElField a
        -> Vinyl.Const T.Text a
    aux (Vinyl.Field x) = Vinyl.Const $ toText x

    
-- | Given a record of functions to map each field to Text,
-- transform a stream of records into a stream of lines of Text,
-- headers first, with headers/fields separated by the given separator.
streamSV'
  :: forall rs t m f.
     (Vinyl.RecordToList rs
     , Vinyl.RApply rs
     , Frames.ColumnHeaders rs
     , IsStream t
     , Monad m
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
liftFieldFormatter toText = Vinyl.Lift $ Vinyl.Const . toText . Vinyl.getField
{-# INLINEABLE liftFieldFormatter #-}

-- | lift a composed-field formatting function into the right form to append to a Rec of formatters
-- | Perhaps to format a parsed file with @Maybe@ or @Either@ composed with @ElField@
liftFieldFormatter1 :: (Functor f, Vinyl.KnownField t)
                    => (f (Vinyl.Snd t) -> T.Text) -- ^ formatting function for things like @Maybe a@
                    -> Vinyl.Lift (->) (f Vinyl.:. Vinyl.ElField) (Vinyl.Const T.Text) t
liftFieldFormatter1 toText = Vinyl.Lift $ Vinyl.Const . toText . fmap Vinyl.getField . Vinyl.getCompose
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
  Streamly.fold (Streamly.File.write fp)
    $ Streamly.Unicode.encodeUtf8
    $ Streamly.adapt
    $ Streamly.concatUnfold Streamly.Unfold.fromList
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
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs
    , MonadCatch m)
    => FilePath -- ^ file path
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs) -- ^ stream of @Maybe :. ElField@ records after parsing.  
readTableMaybe = readTableMaybeOpt Frames.defaultParser
{-# INLINEABLE readTableMaybe #-}

-- | Stream a table from a file path.
-- Results composed with the @Maybe@ functor. Unparsed fields are returned as @Nothing@.
-- NB:  If the inferred/given rs is different from the actual file row-type, things will go awry.
readTableMaybeOpt
    :: forall rs t m.
    (MonadIO m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs
    , MonadCatch m)
    => Frames.ParserOptions -- ^ parsing options
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
     (MonadIO m
     , IsStream t
     , Vinyl.RMap rs
     , Frames.ReadRec rs
     , MonadCatch m)
  => FilePath -- ^ file path
  -> t m (Vinyl.Rec (Either T.Text Vinyl.:. Vinyl.ElField) rs) -- ^ stream of @Either :. ElField@ records after parsing. 
readTableEither = readTableEitherOpt Frames.defaultParser

-- | Stream a table from a file path.
-- Results composed with the @Either Text@ functor. Unparsed fields are returned as a @Left@
-- containing the string that failed to parse.
-- NB:  If the inferred/given rs is different from the actual file row-type, things will go awry.
readTableEitherOpt
  :: forall rs t m.
     (MonadIO m
     , IsStream t
     , Vinyl.RMap rs
     , Frames.ReadRec rs
     , MonadCatch m)
  => Frames.ParserOptions -- ^ parsing options
  -> FilePath -- ^ file path
  -> t m (Vinyl.Rec (Either T.Text Vinyl.:. Vinyl.ElField) rs) -- ^ stream of @Either :. ElField@ records after parsing. 
readTableEitherOpt opts = streamTableEitherOpt opts . word8ToTextLines . Streamly.File.toBytes 
{-# INLINEABLE readTableEitherOpt #-}


-- | Stream Table from a file path, dropping rows where any field fails to parse
-- | Use default options
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
readTable
  :: forall rs t m.
     (MonadIO m
     , IsStream t
     , Vinyl.RMap rs
     , Frames.ReadRec rs
     , MonadCatch m)
  => FilePath -- ^ file path
  -> t m (Frames.Record rs) -- ^ stream of Records
readTable = readTableOpt Frames.defaultParser
{-# INLINEABLE readTable #-}

-- | Stream Table from a file path, dropping rows where any field fails to parse
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
readTableOpt
  :: forall rs t m.
     (MonadIO m
     , IsStream t
     , Vinyl.RMap rs
     , Frames.ReadRec rs
     , MonadCatch m)
  => Frames.ParserOptions  -- ^ parsing options
  -> FilePath -- ^ file path
  -> t m (Frames.Record rs)  -- ^ stream of Records
readTableOpt opts = streamTableOpt opts . word8ToTextLines . Streamly.File.toBytes 
{-# INLINEABLE readTableOpt #-}

-- | Convert a stream of lines of `Text` to a table
-- Each field is returned in an @Either Text@ functor. @Right a@ for successful parses
-- and @Left Text@ when parsing fails, containing the text that failed to Parse.
--
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
streamTableEither
    :: forall rs t m.
    (Monad m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs)
    => t m T.Text -- ^ stream of 'Text' rows
    -> t m (Vinyl.Rec ((Either T.Text) Vinyl.:. Vinyl.ElField) rs) -- ^ stream of parsed @Either :. ElField@ rows
streamTableEither = streamTableEitherOpt Frames.defaultParser
{-# INLINEABLE streamTableEither #-}

-- | Convert a stream of lines of `Text` to records.
-- Each field is returned in an @Either Text@ functor. @Right a@ for successful parses
-- and @Left Text@ when parsing fails, containing the text that failed to Parse.
--
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will..go awry.
streamTableEitherOpt
    :: forall rs t m.
    (Monad m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs)
    => Frames.ParserOptions -- ^ parsing options
    -> t m T.Text -- ^ stream of 'Text' rows
    -> t m (Vinyl.Rec ((Either T.Text) Vinyl.:. Vinyl.ElField) rs)  -- ^ stream of parsed @Either :. ElField@ rows
streamTableEitherOpt opts =
    Streamly.map (doParse . Frames.tokenizeRow opts)
    . handleHeader
  where
    handleHeader | isNothing (Frames.headerOverride opts) = Streamly.drop 1
                 | otherwise                       = id
    doParse = Frames.readRec    
{-# INLINEABLE streamTableEitherOpt #-}

-- | Convert a stream of lines of `Text` to a table.
--
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will..go awry.
streamTableMaybe
    :: forall rs t m.
    (Monad m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs)
    => t m T.Text -- ^ stream of 'Text' rows 
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs) -- ^ stream of parsed @Maybe :. ElField@ rows
streamTableMaybe = streamTableMaybeOpt Frames.defaultParser 
{-# INLINEABLE streamTableMaybe #-}

-- | Convert a stream of lines of Text to a table .
--
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will..go awry.
streamTableMaybeOpt
    :: forall rs t m.
    (Monad m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs)
    => Frames.ParserOptions -- ^ parsing options
    -> t m T.Text -- ^ stream of 'Text' rows
    -> t m (Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs) -- ^ stream of parsed @Maybe :. ElField@ rows
streamTableMaybeOpt opts = Streamly.map recEitherToMaybe . streamTableEitherOpt opts
{-# INLINEABLE streamTableMaybeOpt #-}

-- | Convert a stream of lines of 'Text' to a table,
-- dropping rows where any field fails to parse.
-- Use default options.
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
streamTable
    :: forall rs t m.
    (Monad m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs
    )
    => t m T.Text -- ^ stream of 'Text' rows
    -> t m (Frames.Record rs) -- ^ stream of Records
streamTable = streamTableOpt Frames.defaultParser
{-# INLINEABLE streamTable #-}

-- | Convert a stream of lines of 'Text' `Word8` to a table,
-- dropping rows where any field fails to parse.
-- NB:  If the inferred/given @rs@ is different from the actual file row-type, things will go awry.
streamTableOpt
    :: forall rs t m.
    (Monad m
    , IsStream t
    , Vinyl.RMap rs
    , Frames.ReadRec rs
    )
    => Frames.ParserOptions -- ^ parsing options
    -> t m T.Text  -- ^ stream of 'Text' rows
    -> t m (Frames.Record rs) -- ^ stream of Records
streamTableOpt opts =
    Streamly.mapMaybe (Frames.recMaybe . doParse . Frames.tokenizeRow opts)
    . handleHeader    
  where
    handleHeader | isNothing (Frames.headerOverride opts) = Streamly.drop 1
                 | otherwise                       = id
    doParse = recEitherToMaybe . Frames.readRec
{-# INLINE streamTableOpt #-}

recEitherToMaybe :: Vinyl.RMap rs => Vinyl.Rec (Either T.Text Vinyl.:. Vinyl.ElField) rs -> Vinyl.Rec (Maybe Vinyl.:. Vinyl.ElField) rs
recEitherToMaybe = Vinyl.rmap (either (const (Vinyl.Compose Nothing)) (Vinyl.Compose . Just) . Vinyl.getCompose)
{-# INLINE recEitherToMaybe #-}

-- | Convert a stream of Word8 to lines of `Text` by decoding as UTF8 and splitting on "\n"
word8ToTextLines :: (IsStream t, Monad m) => t m Word8 -> t m T.Text
word8ToTextLines =  Streamly.splitOnSuffix (== '\n') (fmap T.pack $ Streamly.Fold.toList)
                    . Streamly.Unicode.decodeUtf8
{-# INLINE word8ToTextLines #-}


-- tracing fold
{-
runningCountF :: MonadIO m => T.Text -> (Int -> T.Text) -> T.Text -> Streamly.Fold.Fold m a ()
runningCountF startMsg countMsg endMsg = Streamly.Fold.Fold step start done where
  start = liftIO (T.putStr startMsg) >> return 0
  step !n _ = liftIO $ do
    t <- System.Clock.getTime System.Clock.ProcessCPUTime
    putStr $ show t ++ ": "
    T.putStrLn $ countMsg n
    return (n+1)
  done _ = liftIO $ T.putStrLn endMsg
-}

