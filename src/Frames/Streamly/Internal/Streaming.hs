{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveLift #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}
module Frames.Streamly.Internal.Streaming where

import Frames.Streamly.Internal.CSV (FramesCSVException(..))
import           Control.Monad.Catch                     ( MonadThrow(..), MonadCatch)
import qualified Data.Text as T
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
  , sCons :: forall a. a -> s m a -> s m a
  -- ^ add an element to the head of a stream
  , sUncons :: forall a . s m a -> m (Maybe (a, s m a))
    -- ^ split a stream into it's head and tail, returning @m Nothing@ if the stream was empty
  , sMap :: forall x y. (x -> y) -> s m x -> s m y
    -- ^ map each element of the stream using the given function
  , sMapMaybe :: forall x y. (x -> Maybe y) -> s m x -> s m y
    -- ^ map each element of the stream using the given function
  , sDrop :: forall a.Int -> s m a -> s m a
    -- ^ drop n items from the head of the stream
  , sFromEffect :: forall a.m a -> s m a
    -- ^ lift a monadic action returning a into a stream
  , sFolder :: forall x b. (x -> b -> x) -> x -> s m b -> m x
    -- ^ fold the stream using the given step function and starting value
  , sToList :: forall x. s m x -> m [x]
  -- ^ stream to (lazy) list
  , sFromFoldable :: forall f a.Foldable f => f a -> s m a
    -- ^ build a stream of @a@ from a foldable of @a@
  , sUnfoldList :: forall a.s m [a] -> s m a
  -- ^ unfold a stream of lists of a into one stream of a
  , sTextLines :: FilePath -> s m Text
    -- ^ create a stream of lines of text by reading the given file
  , sLineReader :: (Text -> [Text]) -> s m [Text]
    -- ^ given a function to split a line of 'Text' into @[Text]@ items, produce a stream of @[Text]@.
    -- This function needs to be bound to a source (a file or some such).
  , sEncodeUtf8 :: s m Char -> s m Word 8
    -- ^ streamly version handles invalid characters
  }

{-
import qualified Pipes
import Pipes.Lift (lift)

pipesFromEffect :: m a -> Producer a m ()
pipesFromEffect ma = lift ma >>= Pipes.yield

pipesFromFoldable :: (Functor m, Foldable f) => f a -> Pipes.Producer a m ()
pipesFromFoldable fa = Pipes.each

pipesUnfoldList ::  Producer [a] m x -> Producer a m x
pipesUnfoldList = ??

pipesEncodeUtf8 :: Producer Char m x -> Producer Word8 m x
pipesEncodeUtf8 = ??
-}



streamlyFunctions :: (Streamly.MonadAsync m, MonadCatch m) => FilePath -> StreamFunctions Streamly.SerialT m
streamlyFunctions fp = StreamFunctions
  streamlyThrowIfEmpty
  Streamly.cons
  Streamly.uncons
  Streamly.map
  Streamly.mapMaybe
  Streamly.drop
  fromEffect
  streamlyFolder
  Streamly.toList
  Streamly.fromFoldable
  streamlyUnfoldList
  streamTextLines
  (\f -> Streamly.map f $ streamTextLines fp)
  Streamly.Unicode.encodeUtf8

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
