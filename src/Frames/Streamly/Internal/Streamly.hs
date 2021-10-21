{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
module Frames.Streamly.Internal.Streamly
  (
    streamlyFunctions
  , SerialT
  )
where

import Frames.Streamly.Internal.Streaming (StreamFunctions(..))

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
#else
import qualified Streamly                               as Streamly
import           Streamly                                ( IsStream, SerialT )
import qualified Streamly.Internal.Memory.Unicode.Array as Streamly.Unicode.Array
import qualified Streamly.Internal.Memory.Array.Types as Streamly.Array
import qualified Streamly.Data.Unicode.Stream           as Streamly.Unicode
import qualified Streamly.Internal.Data.Fold as Streamly.Fold
#endif

streamlyFunctions :: (Streamly.MonadAsync m, MonadCatch m) => StreamFunctions Streamly.SerialT m
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
  Streamly.toList
  Streamly.fromFoldable
  streamTextLines
  streamlyWriteTextLines

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

fromEffect :: (Monad m, IsStream t) => m a -> t m a
#if MIN_VERSION_streamly(0,8,0)
fromEffect = Streamly.fromEffect
#else
fromEffect = Streamly.yieldM
#endif
{-# INLINE fromEffect #-}
{-# INLINEABLE streamlyFunctions #-}

streamlyUnfoldList :: (IsStream t, Monad m) => t m [a] -> t m a
#if MIN_VERSION_streamly(0,8,0)
streamlyUnfoldList = Streamly.unfoldMany Streamly.Unfold.fromList
#else
streamlyUnfoldList = Streamly.concatUnfold Streamly.Unfold.fromList
#endif
{-# INLINE streamlyUnfoldList #-}

streamlyThrowIfEmpty :: MonadThrow m => Streamly.SerialT m a -> m ()
streamlyThrowIfEmpty s = Streamly.null s >>= flip when (throwM EmptyStreamException)
{-# INLINE streamlyThrowIfEmpty #-}

streamlyFolder :: Monad m => (x -> a -> x) -> x -> Streamly.SerialT m a -> m x
streamlyFolder step start = Streamly.fold fld where
#if MIN_VERSION_streamly(0,8,0)
  fld = Streamly.Fold.foldl' step start
#else
  fld = Streamly.Fold.mkPure step start id
#endif

streamWord8 :: (Streamly.IsStream t, Streamly.MonadAsync m, MonadCatch m) => FilePath -> t m Word8
streamWord8 =  Streamly.File.toBytes
{-# INLINE streamWord8 #-}

streamTextLines :: (Streamly.IsStream t, Streamly.MonadAsync m, MonadCatch m) => FilePath -> t m Text
streamTextLines = word8ToTextLines2 . streamWord8
{-# INLINE streamTextLines #-}

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
