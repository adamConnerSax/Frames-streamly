{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveLift #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
module Frames.Streamly.Internal.Streaming where

import Frames.Streamly.Internal.CSV (FramesCSVException(..))
import           Control.Monad.Catch                     ( MonadThrow(..), MonadCatch)
import qualified Data.Text as T
import qualified Data.Vinyl as V
import qualified Streamly.Prelude                       as Streamly
import qualified Streamly.Data.Fold                     as Streamly.Fold
import qualified Streamly.Internal.FileSystem.File      as Streamly.File
import qualified Streamly.Internal.Data.Unfold          as Streamly.Unfold
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

data StreamFunctions s m = StreamFunctions
  { sThrowIfEmpty :: forall x. s m x -> m ()
    -- ^ throw an exception if the stream is empty
  , sMapper :: forall x y. (x -> y) -> s m x -> s m y
  -- ^ map each element of the stream using the given function
  , sFolder :: forall x b. (x -> b -> x) -> x -> s m b -> m x
  -- ^ fold the stream using the given step function and starting value
  , sUncons :: forall a . s m a -> m (Maybe (a, s m a))
  -- ^ split a stream into it's head and tail, returning @m Nothing@ if the stream was empty
  , sTextLines :: FilePath -> s m Text
  -- ^ create a stream of lines of text by reading the given file
  , sLineReader :: (Text -> [Text]) -> s m [Text]
  -- ^ given a function to split a line of 'Text' into @[Text]@ items, produce a stream of @[Text]@.
  -- This function needs to be bound to a source (a file or some such).
  , sDrop :: forall a.Int -> s m a -> s m a
  -- ^ drop n items from the head of the stream
--  , sParsed :: forall a. ([Text] -> a) -> s m a
  , sFromEffect :: forall a.m a -> s m a
    -- ^ lift a monadic action returning a into a stream
  }

{-
import qualified Pipes
import Pipes.Lift (lift)

pipesFromEffect :: m a -> Producer a m ()
pipesFromEffect ma = lift ma >>= Pipes.yield
-}

streamlyFunctions :: (Streamly.MonadAsync m, MonadCatch m) => FilePath -> StreamFunctions Streamly.SerialT m
streamlyFunctions fp = StreamFunctions
  streamlyThrowIfEmpty
  Streamly.map
  streamlyFolder
  Streamly.uncons
  streamTextLines
  (\f -> Streamly.map f $ streamTextLines fp)
  Streamly.drop
  fromEffect

fromEffect :: (Monad m, IsStream t) => m a -> t m a
#if MIN_VERSION_streamly(0,8,0)
fromEffect = Streamly.fromEffect
#else
fromEffect = Streamly.yieldM
#endif
{-# INLINE fromEffect #-}
{-# INLINEABLE streamlyFunctions #-}

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

{-
streamTokenized' :: (Streamly.IsStream t, Streamly.MonadAsync m, MonadCatch m) => FilePath -> Frames.Separator -> t m [Text]
streamTokenized' fp sep =  Streamly.map (fmap T.copy . Frames.tokenizeRow popts) $ streamTextLines fp where
  popts = Frames.defaultParser { Frames.columnSeparator = sep }
{-# INLINE streamTokenized' #-}

streamTokenized :: (Streamly.IsStream t, Streamly.MonadAsync m, MonadCatch m) => FilePath -> t m [Text]
streamTokenized =  Streamly.map (fmap T.copy . Frames.tokenizeRow Frames.defaultParser) . streamTextLines
{-# INLINE streamTokenized #-}
-}
{-
streamParsed :: (V.RMap rs, StrictReadRec rs, Streamly.IsStream t, Streamly.MonadAsync m, MonadCatch m)
  => FilePath -> t m (V.Rec (Strict.Either Text V.:. V.ElField) rs)
streamParsed =  Streamly.map (strictReadRec . Frames.tokenizeRow Frames.defaultParser) . streamTextLines
{-# INLINE streamParsed #-}

streamParsedMaybe :: (V.RMap rs, StrictReadRec rs) => (Streamly.IsStream t, Streamly.MonadAsync m, MonadCatch m) => FilePath -> t m (V.Rec (Maybe V.:. V.ElField) rs)
streamParsedMaybe =  Streamly.map (recStrictEitherToMaybe . strictReadRec . Frames.tokenizeRow Frames.defaultParser) . streamTextLines
{-# INLINE streamParsedMaybe #-}
-}
