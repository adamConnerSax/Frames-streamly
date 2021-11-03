{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
module Frames.Streamly.Streaming.Streamly
  (
    StreamlyStream(..)
    -- * re-exports
  , SerialT
  , IsStream
  )
where

import Frames.Streamly.Streaming.Class
import qualified Frames.Streamly.Streaming.Common as Common

import Frames.Streamly.Internal.CSV (FramesCSVException(..))
import           Control.Monad.Catch                     ( MonadThrow(..), MonadCatch)
import Control.Foldl (PrimMonad)
import Control.Exception (try)
import qualified Control.Monad.Trans.Control as MC
import qualified Data.Text as T
import qualified Data.Text.Encoding as Text
import Data.Word8 (_lf)


import qualified Streamly.Prelude                       as Streamly
import qualified Streamly.Data.Fold                     as Streamly.Fold
import qualified Streamly.Internal.FileSystem.File      as Streamly.File
import qualified Streamly.Internal.FileSystem.Handle    as Streamly.Handle
import qualified Streamly.Internal.Data.Unfold          as Streamly.Unfold
import qualified Streamly.External.ByteString as Streamly.BS
#if MIN_VERSION_streamly(0,8,0)
import Streamly.Prelude                       (IsStream, SerialT)
import qualified Streamly.Unicode.Stream           as Unicode
import qualified Streamly.Internal.Data.Array.Stream.Foreign as Array.Stream
import qualified Streamly.Internal.Data.Stream.StreamD.Generate as StreamD
import qualified Streamly.Internal.Data.Stream.StreamD.Type as StreamD
import qualified Streamly.Internal.Data.Stream.StreamD.Transform as StreamD
#else
import qualified Streamly                               as Streamly
import           Streamly                                ( IsStream, SerialT )
import qualified Streamly.Data.Unicode.Stream           as Streamly.Unicode
import qualified Streamly.Internal.Data.Fold as Streamly.Fold
#endif

import qualified Data.Text.IO as Text
import qualified System.IO as IO
import GHC.IO.Exception (IOException)

newtype StreamlyStream (t ::  (Type -> Type) -> Type -> Type) m a = StreamlyStream { stream :: t m a }

instance (IsStream t, Monad m) => StreamFunctions (StreamlyStream t) m where
  type FoldType (StreamlyStream t) = Streamly.Fold.Fold
  sThrowIfEmpty = streamlyThrowIfEmpty . stream
  sLength = Streamly.length . Streamly.adapt . stream
  sCons a = StreamlyStream . Streamly.cons a . stream
  sUncons = streamlyStreamUncons
  sHead = Streamly.head . Streamly.adapt . stream
  sMap f = StreamlyStream . Streamly.map f . stream
  sMapMaybe f = StreamlyStream . Streamly.mapMaybe f . stream
  sScanM step start = StreamlyStream . Streamly.scanlM' step start . stream
  sDrop n = StreamlyStream . Streamly.drop n . stream
  sTake n = StreamlyStream . Streamly.take n . stream
  sFolder step start = streamlyFolder step start . stream
  sBuildFold = streamlyBuildFold
  sBuildFoldM = streamlyBuildFoldM
  sMapFoldM = Streamly.Fold.rmapM
  sLMapFoldM = Streamly.Fold.lmapM
  sFoldMaybe = Streamly.Fold.catMaybes
  sFold fld  = Streamly.fold fld . Streamly.adapt . stream
  sToList = Streamly.toList . Streamly.adapt . stream -- this might be bad (not lazy) compared to streamly
  sFromFoldable = StreamlyStream . Streamly.fromFoldable

  {-# INLINEABLE sThrowIfEmpty #-}
  {-# INLINEABLE sLength #-}
  {-# INLINEABLE sCons #-}
  {-# INLINEABLE sUncons #-}
  {-# INLINEABLE sHead #-}
  {-# INLINEABLE sMap #-}
  {-# INLINEABLE sMapMaybe #-}
  {-# INLINEABLE sScanM #-}
  {-# INLINEABLE sDrop #-}
  {-# INLINEABLE sTake #-}
  {-# INLINEABLE sFolder #-}
  {-# INLINEABLE sBuildFold #-}
  {-# INLINEABLE sBuildFoldM #-}
  {-# INLINEABLE sMapFoldM #-}
  {-# INLINEABLE sLMapFoldM #-}
  {-# INLINEABLE sFoldMaybe #-}
  {-# INLINEABLE sFold #-}
  {-# INLINEABLE sToList #-}
  {-# INLINEABLE sFromFoldable #-}

instance (IsStream t, Streamly.MonadAsync m, MonadCatch m, PrimMonad m) => StreamFunctionsIO (StreamlyStream t) m where
  type IOSafe (StreamlyStream t) m = m
  runSafe = id
  sReadTextLines = StreamlyStream . streamlyReadTextLines linesUsingSplitOn
  sTokenized sep qm = StreamlyStream . streamlyReadTextLines (tokenized sep qm)
  sReadScanMAndFold = streamlyReadScanMAndFold
  sWriteTextLines fp = streamlyWriteTextLines fp . stream

  {-# INLINEABLE runSafe #-}
  {-# INLINEABLE sReadTextLines #-}
  {-# INLINEABLE sTokenized #-}
  {-# INLINEABLE sReadScanMAndFold #-}
  {-# INLINEABLE sWriteTextLines #-}

streamlyStreamUncons :: (IsStream t, Monad m) => StreamlyStream t m a -> m (Maybe (a, StreamlyStream t m a))
streamlyStreamUncons s = do
  unc <- Streamly.uncons (Streamly.adapt $ stream s)
  case unc of
    Nothing -> return Nothing
    Just (a, s') -> return $ Just (a, StreamlyStream s')
{-# INLINABLE streamlyStreamUncons #-}

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
    $ Unicode.encodeUtf8
    $ Streamly.adapt
    $ unfoldMany Streamly.Unfold.fromList
    $ Streamly.map T.unpack
    $ Streamly.intersperse "\n" s
{-# INLINEABLE streamlyWriteTextLines #-}

-- Use Text to read a line at a time
streamlyUnfoldTextLn :: MonadIO m => Streamly.Unfold.Unfold m IO.Handle Text
streamlyUnfoldTextLn = Streamly.Unfold.unfoldrM f where
  getOne :: IO.Handle -> IO (Either IOException Text)
  getOne h = try (Text.hGetLine h)
  f h = do
    tE <- liftIO $ getOne h
    case tE of
      Left _ -> return Nothing
      Right t -> return $ Just (t, h)
{-# INLINE streamlyUnfoldTextLn #-}


streamlyReadTextLines :: (Streamly.IsStream t, Streamly.MonadAsync m, MonadCatch m)
                      => (IO.Handle -> t m a) -> FilePath -> t m a
streamlyReadTextLines f fp = Streamly.bracket (liftIO $ IO.openFile fp IO.ReadMode) (liftIO . IO.hClose) f
--                           $ Streamly.unfold streamlyUnfoldTextLn
{-# INLINE streamlyReadTextLines #-}

withFileLifted :: MC.MonadBaseControl IO m => FilePath -> IOMode -> (Handle -> m a) -> m a
withFileLifted file mode action = MC.liftBaseWith (\runInBase -> withFile file mode (runInBase . action)) >>= MC.restoreM
{-# INLINEABLE withFileLifted #-}

streamlyReadScanMAndFold :: Streamly.MonadAsync m => FilePath -> (x -> Text -> m x) -> m x -> Streamly.Fold.Fold m x b -> m b
streamlyReadScanMAndFold fp scanStep scanStart fld = withFileLifted fp IO.ReadMode
  $ StreamD.fold fld . StreamD.scanlM' scanStep scanStart . StreamD.unfold streamlyUnfoldTextLn
{-# INLINEABLE streamlyReadScanMAndFold #-}

streamlyThrowIfEmpty :: (IsStream t, MonadThrow m) => t m a -> m ()
streamlyThrowIfEmpty s = Streamly.null (Streamly.adapt s) >>= flip when (throwM EmptyStreamException)
{-# INLINE streamlyThrowIfEmpty #-}

streamlyFolder :: (IsStream t, Monad m) => (x -> a -> x) -> x -> t m a -> m x
streamlyFolder step start = Streamly.fold (streamlyBuildFold step start id) . Streamly.adapt
{-# INLINABLE streamlyFolder #-}


linesUsingSplitOn :: (IsStream t, MonadIO m) => IO.Handle -> t m Text
linesUsingSplitOn h = Streamly.unfold Streamly.Handle.readChunks h
                      & Array.Stream.splitOnSuffix _lf
                      & Streamly.map (Text.decodeUtf8 . Streamly.BS.fromArray)
{-# INLINE linesUsingSplitOn #-}


tokenized :: (IsStream t, MonadIO m) => Common.Separator -> Common.QuotingMode -> IO.Handle -> t m [Text]
tokenized sep qm h = Streamly.unfold Streamly.Handle.readChunks h
                     & Array.Stream.splitOnSuffix _lf
                     & Streamly.map (Common.tokenizeRow sep qm . Text.decodeUtf8 . Streamly.BS.fromArray)
{-# INLINE tokenized #-}

{-
tokenized2 :: (IsStream t, Streamly.MonadAsync m) => Common.Separator -> Common.QuotingMode -> IO.Handle -> t m [Text]
tokenized2 sep qm h = Streamly.unfold Streamly.Handle.readChunks h
                     & Array.Stream.splitOnSuffix _lf
                     & wordArraysToTextLists sep qm
{-# INLINE tokenized2 #-}

wordArraysToTextLists :: (IsStream t, Streamly.MonadAsync m) => Common.Separator -> Common.QuotingMode -> t m (Array.Array Word8) -> t m [Text]
wordArraysToTextLists sep qm = case sep of
  Common.TextSeparator _ ->  Streamly.map (Common.tokenizeRow sep qm . Text.decodeUtf8 . Streamly.BS.fromArray)
  Common.CharSeparator c -> Streamly.mapM (fmap (Common.handleQuoting sep qm) . processArray2 c)
{-# INLINE wordArraysToTextLists #-}

wordArraysToTextListsRaw :: (IsStream t, Streamly.MonadAsync m) => Common.Separator -> t m (Array.Array Word8) -> t m [Text]
wordArraysToTextListsRaw sep = case sep of
  Common.TextSeparator _ ->  Streamly.map (Common.splitRow sep . Text.decodeUtf8 . Streamly.BS.fromArray)
  Common.CharSeparator c -> Streamly.mapM (processArray2 c)
{-# INLINE wordArraysToTextListsRaw #-}

tokenizedRaw :: (IsStream t, MonadIO m) => Common.Separator -> IO.Handle -> t m [Text]
tokenizedRaw sep h = Streamly.unfold Streamly.Handle.readChunks h
                     & Array.Stream.splitOnSuffix _lf
                     & Streamly.map (Common.splitRow sep . Text.decodeUtf8 . Streamly.BS.fromArray)
{-# INLINE tokenizedRaw #-}

tokenizedRaw3 :: forall t m.(IsStream t, Streamly.MonadAsync m) => Common.Separator -> IO.Handle -> t m [Text]
tokenizedRaw3 sep h = Streamly.unfold Streamly.Handle.readChunks h
                      & Array.Stream.splitOnSuffix _lf
                      & wordArraysToTextListsRaw sep
{-# INLINE tokenizedRaw3 #-}

{-# INLINE processArray2 #-}
processArray2 :: MonadIO m => Char ->  Array.Array Word8 -> m [Text]
processArray2 c arr =
  let c2w = fromIntegral . ord
      toText arr = Text.decodeUtf8 $ Streamly.BS.fromArray arr
  in Streamly.toList $ Streamly.map toText $ Array.Stream.splitOn (c2w c) $ Streamly.fromPure arr

tokenizedRaw2 :: forall t m.(IsStream t, Streamly.MonadAsync m) => Common.Separator -> IO.Handle -> t m [Text]
tokenizedRaw2 sep h = Streamly.unfold Streamly.Handle.readChunks h & processArrayStream sep
{-# INLINE tokenizedRaw2 #-}

{-# INLINE toTextFld #-}
toTextFld :: MonadIO m => Streamly.Fold.Fold m Word8 Text
toTextFld = fmap (Text.decodeUtf8With Text.lenientDecode) Streamly.BS.write

{-# INLINE lineFold #-}
lineFold :: MonadIO m => Char -> Streamly.Fold.Fold m Word8 [Text]
lineFold c =
  let c2w = fromIntegral . ord
      split = Streamly.Fold.takeEndBy (== c2w c) toTextFld
  in Streamly.Fold.many split Streamly.Fold.toList




{-# INLINE processArray #-}
processArray :: MonadIO m => Char -> Array.Array Word8 -> m [Text]
processArray c arr = Streamly.unfold Array.read arr
                     & Streamly.fold (lineFold c)

{-# INLINE processArrayBaseLine #-}
processArrayBaseLine :: Char -> Array.Array Word8 -> [Text]
processArrayBaseLine c arr =  Common.splitRow (Common.CharSeparator c) $ Text.decodeUtf8 $ Streamly.BS.fromArray arr

{-# INLINE processArrayBaseLine2 #-}
processArrayBaseLine2 :: MonadIO m => Char -> Array.Array Word8 -> m [Text]
processArrayBaseLine2 c arr =  Common.splitRow (Common.CharSeparator c) <$> Array.fold toTextFld arr


{-# INLINE processArrayStream #-}
processArrayStream :: (Streamly.MonadAsync m, IsStream t) => Common.Separator -> t m (Array.Array Word8) -> t m [Text]
processArrayStream sep arrS =
  let arrayToText = case sep of
        Common.TextSeparator _ ->  Streamly.map (Common.splitRow sep . Text.decodeUtf8 . Streamly.BS.fromArray)
        Common.CharSeparator c -> Streamly.mapM (processArray2 c)
  in arrayToText $ Array.Stream.splitOnSuffix _lf arrS
-}
{-
tokenizedLines :: (IsStream t, MonadIO m) => Word8 -> IO.Handle -> t m Text
tokenizedLines s h = Streamly.unfold Streamly.Handle.readChunks h
                      & Streamly.Array.splitOnSuffix _lf
                      & Streamly.filter (not . F.null)
                      & Streamly.map (Text.decodeUtf8 . Streamly.BS.fromArray . S)
{-# INLINE tokenizedLines #-}
-}
{-
streamlyReadTextLines' :: (Streamly.IsStream t, Streamly.MonadAsync m, MonadCatch m) => FilePath -> t m Text
streamlyReadTextLines' = word8ToTextLines2 . streamWord8
{-# INLINE streamlyReadTextLines' #-}

streamWord8 :: (Streamly.IsStream t, Streamly.MonadAsync m, MonadCatch m) => FilePath -> t m Word8
streamWord8 =  Streamly.File.toBytes
{-# INLINE streamWord8 #-}

word8ToTextLines2 :: (IsStream t, MonadIO m) => t m Word8 -> t m T.Text
word8ToTextLines2 =  Streamly.map (T.pack . Streamly.Array.toList)
                     . Streamly.Unicode.Array.lines
                     . Streamly.Unicode.decodeUtf8
{-# INLINE word8ToTextLines2 #-}


-- | Convert a stream of Word8 to lines of `Text` by decoding as UTF8 and splitting on "\n"
word8ToTextLines :: (IsStream t, MonadIO m) => t m Word8 -> t m T.Text
word8ToTextLines =  Streamly.splitOnSuffix(=='\n') (toText <$> Streamly.Fold.toList)
                    . Streamly.Unicode.decodeUtf8
{-# INLINE word8ToTextLines #-}
-}

{-
-- use streamly to read Word8 and then parse to lines
textLinesFromHandle :: (MonadThrow m, IsStream t) => MonadIO m => IO.Handle -> t m Text
textLinesFromHandle = Reduce.parseManyD newlineParserD . Streamly.unfold Streamly.Handle.read
{-# INLINE textLinesFromHandle #-}


foldWord8ToText :: Monad m => Streamly.Fold.Fold m Word8 Text
foldWord8ToText = fmap (T.pack . fmap w2c) $ Streamly.Fold.toList
{-# INLINE foldWord8ToText #-}

foldWord8ToText2 :: Monad m => Streamly.Fold.Fold m Word8 Text
foldWord8ToText2 = fmap TB.run $ Streamly.Fold.foldl' (\b w -> b <> TB.char (w2c w)) mempty
{-# INLINE foldWord8ToText2 #-}


newlineParserD :: (MonadIO m, MonadThrow m) => ParserD.Parser m Word8 Text
newlineParserD = ParserD.takeWhile (/= _lf) foldWord8ToText2 <* next
{-# INLINE newlineParserD #-}

data PState = PState !FArray.Array Word8 [Text]

arrayWordStreamToTextStream :: t m (FArray.Array Word8) -> t m Text
arrayWordStreamToTextStream = Reduce.foldIterateM g initial where
  g :: PState -> m (Streamly.Fold (FArray.Array Word8) PState)
  g (PState remainder lines) = Streamly.Fold.runStep fld remainder where
    fld :: Streamly.Fold (FArray.Array Word8) PState = Streamly.Fold.foldlM' step p0 where
      step :: PState -> FArray.Array Word8 -> m PState
      step (PState remainder lines) cur = do
        newChunk <- FArray.spliceTwo remainder cur
        (newRemainder, newLines) <- breakAll newChunk
        return $ PState newRemainder newLines
      p0 = PState mempty []
  initial :: PState
  initial = PState mempty []


breakAll :: FArray.Array Word8 -> m (FArray.Array Word8, [FArray.Array Word8])
breakAll a = go (a, []) where
  go a l = do
    ma <- FArray.breakOn _lf a
    case ma of
      (prefix, Just suffix) -> go (prefix, suffix : l)
      (prefix, Nothing) -> return (prefix, l)
{-# INLINE breakAllOn #-}

next :: Monad m => ParserD.Parser m a (Maybe a)
next = ParserD.Parser step initial extract
  where
  initial = pure $ ParserD.IPartial ()
  step _ a = pure $ ParserD.Done 0 (Just a)
  extract _ = pure Nothing
{-# INLINE next #-}
-}

{-
lines :: BL.ByteString -> DL.DList (BL.ByteString)
lines = DL.unfoldr inner
  where
    {-# INLINE inner #-}
    inner input'
      | BL.null input' = Nothing
      | otherwise =
          case BL.elemIndex _lf input' of
            Nothing -> Just (input', BL.empty)
            Just i ->
              let (prefix, suffix) = BL.splitAt i input'
              in Just (prefix, BL.drop 1 suffix)
{-# INLINE lines #-}

unfoldViaLBS' :: Applicative m => Word8 -> Streamly.Unfold.Unfold m BL.ByteString BL.ByteString
unfoldViaLBS' w = Streamly.Unfold.unfoldr inner
  where
    {-# INLINE inner #-}
    inner input'
      | BL.null input' = Nothing
      | otherwise =
          case BL.elemIndex w input' of
            Nothing -> Just (input', BL.empty)
            Just i ->
              let (prefix, suffix) = BL.splitAt i input'
              in Just (prefix, BL.drop 1 suffix)
{-# INLINE unfoldViaLBS' #-}

unfoldViaSBS' :: Applicative m => Word8 -> Streamly.Unfold.Unfold m BS.ByteString BS.ByteString
unfoldViaSBS' w = Streamly.Unfold.unfoldr inner
  where
    {-# INLINE inner #-}
    inner input'
      | BS.null input' = Nothing
      | otherwise =
          case BS.elemIndex w input' of
            Nothing -> Just (input', BS.empty)
            Just i ->
              let (prefix, suffix) = BS.splitAt i input'
              in Just (prefix, BS.drop 1 suffix)
{-# INLINE unfoldViaSBS' #-}


unfoldViaLBS :: MonadIO m => Word8 -> Streamly.Unfold.Unfold m IO.Handle Text
unfoldViaLBS w = fmap (Text.decodeUtf8 . BL.toStrict) $ Streamly.Unfold.lmapM (liftIO . BL.hGetContents) (unfoldViaLBS' w)
{-# INLINE unfoldViaLBS #-}

readTextLinesRaw :: (IsStream t, MonadIO m) => IO.Handle -> t m Text
readTextLinesRaw = Streamly.foldMany lineFold . Streamly.unfoldMany read' . fileStream where
  fileStream = Streamly.unfold Streamly.Handle.readChunks
{-# INLINE readTextLinesRaw #-}

read' :: MonadIO m => Streamly.Unfold.Unfold m (F.Array Word8) Word8
read' = Streamly.Unfold.lmap F.unsafeThaw FM.read
{-# INLINE [0] read' #-}

lineFold :: Applicative m => Streamly.Fold.Fold m Word8 Text
lineFold = Streamly.Fold.Fold step initial extract
  where
    dlToText = T.pack . fmap w2c . DL.toList
    initial = pure $ Streamly.Fold.Partial DL.empty
    step !s !a
      | a == _lf = pure $ Streamly.Fold.Done $ dlToText s
      | otherwise = pure $ Streamly.Fold.Partial (DL.snoc s a)
    extract !s = pure $ dlToText s
{-# INLINE [2] lineFold #-}


-}
