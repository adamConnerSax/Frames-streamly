{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
module Frames.Streamly.Internal.Streamly
  (
    streamlyFunctions
  , streamlyFunctionsIO
  , streamlyFunctionsWithIO
  , SerialT
  )
where

import Frames.Streamly.Streaming

import Frames.Streamly.Internal.CSV (FramesCSVException(..))
import           Control.Monad.Catch                     ( MonadThrow(..), MonadCatch)

import qualified Data.Text as T
import qualified Streamly.Prelude                       as Streamly
import qualified Streamly.Data.Fold                     as Streamly.Fold
import qualified Streamly.Internal.FileSystem.File      as Streamly.File
import qualified Streamly.Internal.Data.Unfold          as Streamly.Unfold
#if MIN_VERSION_streamly(0,8,0)
import Streamly.Prelude                       (IsStream, SerialT)
import qualified Streamly.Internal.Unicode.Array.Char as Streamly.Unicode.Array
import qualified Streamly.Data.Array.Foreign as Streamly.Array
import qualified Streamly.Unicode.Stream           as Streamly.Unicode
--import qualified Streamly.Internal.Data.Fold.Type as Streamly.Fold
#else
import qualified Streamly                               as Streamly
import           Streamly                                ( IsStream, SerialT )
import qualified Streamly.Internal.Memory.Unicode.Array as Streamly.Unicode.Array
import qualified Streamly.Internal.Memory.Array.Types as Streamly.Array
import qualified Streamly.Data.Unicode.Stream           as Streamly.Unicode
import qualified Streamly.Internal.Data.Fold as Streamly.Fold
#endif

type instance FoldType SerialT = Streamly.Fold.Fold

streamlyFunctions :: MonadThrow m => StreamFunctions Streamly.SerialT m
streamlyFunctions = StreamFunctions
  streamlyThrowIfEmpty
  Streamly.cons
  Streamly.uncons
  Streamly.head
  Streamly.map
  Streamly.mapMaybe
  Streamly.drop
  Streamly.take
  fromEffect
  streamlyFolder
  streamlyBuildFold
  streamlyBuildFoldM
  Streamly.fold
  Streamly.toList
  Streamly.fromFoldable
{-# INLINABLE streamlyFunctions #-}

streamlyFunctionsIO :: (Streamly.MonadAsync m, MonadCatch m)  => StreamFunctionsIO Streamly.SerialT m
streamlyFunctionsIO = StreamFunctionsIO streamlyReadTextLines streamlyWriteTextLines
{-# INLINABLE streamlyFunctionsIO #-}

streamlyFunctionsWithIO :: (Streamly.MonadAsync m, MonadCatch m)  => StreamFunctionsWithIO Streamly.SerialT m
streamlyFunctionsWithIO = StreamFunctionsWithIO streamlyFunctions streamlyFunctionsIO
{-# INLINABLE streamlyFunctionsWithIO #-}


streamlyBuildFold :: Monad m => (x -> a -> x) -> x -> (x -> b) -> Streamly.Fold.Fold m a b
#if MIN_VERSION_streamly(0,8,0)
streamlyBuildFold step start extract = fmap extract $ Streamly.Fold.foldl' step start
#else
streamlyBuildFold step start extract = Streamly.Fold.mkPure step start extract
#endif

streamlyBuildFoldM :: Monad m => (x -> a -> m x) -> m x -> (x -> m b) -> Streamly.Fold.Fold m a b
#if MIN_VERSION_streamly(0,8,0)
streamlyBuildFoldM step start extract = Streamly.Fold.rmapM  extract $ Streamly.Fold.foldlM' step start
#else
streamlyBuildFoldM step start extract = Streamly.Fold.mkFold step start extract
#endif


streamlyWriteTextLines :: (IsStream s, Streamly.MonadAsync m, MonadCatch m) => FilePath -> s m Text -> m ()
streamlyWriteTextLines fp s = do
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
{-# INLINEABLE streamlyWriteTextLines #-}

streamlyReadTextLines :: (Streamly.IsStream t, Streamly.MonadAsync m, MonadCatch m) => FilePath -> t m Text
streamlyReadTextLines = word8ToTextLines2 . streamWord8
{-# INLINE streamlyReadTextLines #-}


fromEffect :: (Monad m, IsStream t) => m a -> t m a
#if MIN_VERSION_streamly(0,8,0)
fromEffect = Streamly.fromEffect
#else
fromEffect = Streamly.yieldM
#endif
{-# INLINE fromEffect #-}

streamlyThrowIfEmpty :: MonadThrow m => Streamly.SerialT m a -> m ()
streamlyThrowIfEmpty s = Streamly.null s >>= flip when (throwM EmptyStreamException)
{-# INLINE streamlyThrowIfEmpty #-}

streamlyFolder :: Monad m => (x -> a -> x) -> x -> Streamly.SerialT m a -> m x
streamlyFolder step start = Streamly.fold (streamlyBuildFold step start id)
{-# INLINABLE streamlyFolder #-}

streamWord8 :: (Streamly.IsStream t, Streamly.MonadAsync m, MonadCatch m) => FilePath -> t m Word8
streamWord8 =  Streamly.File.toBytes
{-# INLINE streamWord8 #-}

word8ToTextLines2 :: (IsStream t, MonadIO m) => t m Word8 -> t m T.Text
word8ToTextLines2 =  Streamly.map (toText . Streamly.Array.toList)
                     . Streamly.Unicode.Array.lines
                     . Streamly.Unicode.decodeUtf8
{-# INLINE word8ToTextLines2 #-}

{-
-- | Convert a stream of Word8 to lines of `Text` by decoding as UTF8 and splitting on "\n"
word8ToTextLines :: (IsStream t, MonadIO m) => t m Word8 -> t m T.Text
word8ToTextLines =  Streamly.splitOnSuffix(=='\n') (toText <$> Streamly.Fold.toList)
                    . Streamly.Unicode.decodeUtf8
{-# INLINE word8ToTextLines #-}
-}
