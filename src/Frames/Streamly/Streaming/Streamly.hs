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
{-# LANGUAGE InstanceSigs #-}
module Frames.Streamly.Streaming.Streamly
  (
    StreamlyStream(..)
    -- * re-exports
  , Stream
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

import qualified Streamly.Data.Fold              as Streamly.Fold
import Streamly.Data.Stream (Stream)
import qualified Streamly.Data.Stream as Streamly
import qualified Streamly.Data.StreamK as StreamK
import qualified Streamly.FileSystem.Path      as Streamly.File
import qualified Streamly.FileSystem.FileIO      as Streamly.File
import qualified Streamly.Unicode.Stream           as Unicode
import qualified Streamly.Data.Unfold          as Streamly.Unfold
import qualified Streamly.FileSystem.Handle    as Streamly.Handle
import qualified Streamly.External.ByteString as Streamly.BS
import qualified Streamly.Internal.Data.Array.Stream as Array.Stream

import qualified Data.Text.IO as Text
import qualified System.IO as IO
import GHC.IO.Exception (IOException)

newtype StreamlyStream (t ::  (Type -> Type) -> Type -> Type) m a = StreamlyStream { stream :: t m a }

instance (Monad m) => StreamFunctions (StreamlyStream Streamly.Stream) m where
  type FoldType (StreamlyStream Streamly.Stream) = Streamly.Fold.Fold
  sThrowIfEmpty = streamlyThrowIfEmpty . stream
  sLength = Streamly.fold Streamly.Fold.length . stream
  sCons a = StreamlyStream . Streamly.cons a . stream
  sUncons = streamlyStreamUncons
  sHead = Streamly.fold Streamly.Fold.one . stream
  sMap f = StreamlyStream . fmap f . stream
  sMapMaybe f = StreamlyStream . Streamly.mapMaybe f . stream
  sScanM step start = StreamlyStream . Streamly.scan (Streamly.Fold.foldlM' step start) . stream
  sDrop :: Monad m => Int -> StreamlyStream Streamly.Stream m a -> StreamlyStream Streamly.Stream m a
  sDrop n = StreamlyStream . Streamly.drop n . stream
  sTake n = StreamlyStream . Streamly.take n . stream
  sFolder step start = streamlyFolder step start . stream
  sBuildFold = streamlyBuildFold
  sBuildFoldM = streamlyBuildFoldM
  sMapFoldM = Streamly.Fold.rmapM
  sLMapFoldM = Streamly.Fold.lmapM
  sFoldMaybe = Streamly.Fold.catMaybes
  sFold fld  = Streamly.fold fld . stream
  sToList = Streamly.toList . stream -- this might be bad (not lazy) compared to streamly
  sFromFoldable = StreamlyStream . StreamK.toStream . StreamK.fromFoldable
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

instance (MonadCatch m, MonadIO m, PrimMonad m, MC.MonadBaseControl IO m) => StreamFunctionsIO (StreamlyStream Streamly.Stream) m where
  type IOSafe (StreamlyStream Streamly.Stream) m = m
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

streamlyStreamUncons :: Monad m => StreamlyStream Streamly.Stream m a -> m (Maybe (a, StreamlyStream Streamly.Stream m a))
streamlyStreamUncons s = do
  unc <- Streamly.uncons (stream s)
  case unc of
    Nothing -> return Nothing
    Just (a, s') -> return $ Just (a, StreamlyStream s')
{-# INLINABLE streamlyStreamUncons #-}

streamlyBuildFold :: Monad m => (x -> a -> x) -> x -> (x -> b) -> Streamly.Fold.Fold m a b
streamlyBuildFold step start extract = fmap extract $ Streamly.Fold.foldl' step start

streamlyBuildFoldM :: Monad m => (x -> a -> m x) -> m x -> (x -> m b) -> Streamly.Fold.Fold m a b
streamlyBuildFoldM step start extract = Streamly.Fold.rmapM  extract $ Streamly.Fold.foldlM' step start

streamlyWriteTextLines :: (MonadCatch m, MonadIO m) => FilePath -> Streamly.Stream m Text -> m ()
streamlyWriteTextLines fp s = do
  path <- Streamly.File.fromString fp
  let unfoldMany = Streamly.unfoldMany
  Streamly.fold (Streamly.File.write path)
    $ Unicode.encodeUtf8
    $ unfoldMany Streamly.Unfold.fromList
    $ fmap T.unpack
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
{-# INLINEABLE streamlyUnfoldTextLn #-}

streamlyReadTextLines :: (MonadCatch m, MonadIO m)
                      => (IO.Handle -> Streamly.Stream m a) -> FilePath -> Streamly.Stream m a
streamlyReadTextLines f fp = Streamly.bracketIO (liftIO $ IO.openFile fp IO.ReadMode) (liftIO . IO.hClose) f
{-# INLINEABLE streamlyReadTextLines #-}

withFileLifted :: MC.MonadBaseControl IO m => FilePath -> IOMode -> (Handle -> m a) -> m a
withFileLifted file mode action = MC.liftBaseWith (\runInBase -> withFile file mode (runInBase . action)) >>= MC.restoreM
{-# INLINEABLE withFileLifted #-}

streamlyReadScanMAndFold :: (MC.MonadBaseControl IO m, MonadIO m) => FilePath -> (x -> Text -> m x) -> m x -> Streamly.Fold.Fold m x b -> m b
streamlyReadScanMAndFold fp scanStep scanStart fld = withFileLifted fp IO.ReadMode
  $ Streamly.fold fld . Streamly.scan (Streamly.Fold.foldlM' scanStep scanStart) . Streamly.unfold streamlyUnfoldTextLn
{-# INLINEABLE streamlyReadScanMAndFold #-}

streamlyThrowIfEmpty :: (MonadThrow m) => Streamly.Stream m a -> m ()
streamlyThrowIfEmpty s = Streamly.fold Streamly.Fold.null s >>= flip when (throwM EmptyStreamException)
{-# INLINEABLE streamlyThrowIfEmpty #-}

streamlyFolder :: Monad m => (x -> a -> x) -> x -> Streamly.Stream m a -> m x
streamlyFolder step start = Streamly.fold (streamlyBuildFold step start id)
{-# INLINABLE streamlyFolder #-}

linesUsingSplitOn :: MonadIO m => IO.Handle -> Streamly.Stream m Text
linesUsingSplitOn h = Streamly.unfold Streamly.Handle.chunkReader h
                      & Array.Stream.splitOnSuffix _lf
                      & fmap (Text.decodeUtf8 . Streamly.BS.fromArray)
{-# INLINEABLE linesUsingSplitOn #-}

tokenized :: MonadIO m => Common.Separator -> Common.QuotingMode -> IO.Handle -> Streamly.Stream m [Text]
tokenized sep qm h = Streamly.unfold Streamly.Handle.chunkReader h
                     & Array.Stream.splitOnSuffix _lf
                     & fmap (Common.tokenizeRow sep qm . Text.decodeUtf8 . Streamly.BS.fromArray)
{-# INLINEABLE tokenized #-}

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
