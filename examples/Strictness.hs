{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
--{-# OPTIONS_GHC -O0 #-}
module Main where

import StrictnessPaths

import qualified Data.Text as T
import qualified Data.Serialize                as S
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Builder as BB
import qualified ByteString.StrictBuilder as BSB
import qualified Control.Foldl                 as FL
import qualified Control.Monad.State           as ST
import qualified Data.Word as Word
import qualified Frames as F
import qualified Frames.Streamly.CSV as FStreamly
import qualified Frames.Streamly.InCore as FStreamly
import qualified Serialize as FS
import qualified Frames.CSV                     as Frames
import qualified Streamly.Data.Fold            as Streamly.Fold
import qualified Streamly.Internal.Data.Fold.Types            as Streamly.Fold
import qualified Streamly.Internal.Data.Fold            as Streamly.Fold
import qualified Streamly.Prelude              as Streamly
import qualified Streamly.Internal.Prelude              as Streamly
import qualified Streamly              as Streamly
import qualified Streamly.Internal.FileSystem.File
                                               as Streamly.File
import qualified Streamly.External.ByteString  as Streamly.ByteString

import qualified Streamly.Internal.Data.Array  as Streamly.Data.Array
import qualified Streamly.Internal.Memory.Array as Streamly.Memory.Array

import qualified System.Clock

import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Control.Monad.Primitive as Prim

import qualified Data.Strict.Either as Strict
import qualified Data.Strict.Maybe as Strict

F.tableTypes' pumsACS1YrRowGen

main :: IO ()
main= do
  testInIO

encodeOne :: S.Serialize a => a -> BB.Builder
encodeOne !x = S.execPut $ S.put x

bldrToCT  = Streamly.ByteString.toArray . BL.toStrict . BB.toLazyByteString

encodeBSB :: S.Serialize a => a -> BSB.Builder
encodeBSB !x = BSB.bytes $! encodeBS x

encodeBS :: S.Serialize a => a -> BS.ByteString
encodeBS !x = S.runPut $! S.put x


bsbToCT  = Streamly.ByteString.toArray . BSB.builderBytes

data Accum b = Accum { count :: !Int, bldr :: !b }

streamlySerializeF :: forall c bldr m a ct.(Monad m, Monoid bldr, c a, c Word.Word64)
                   => (forall b. c b => b -> bldr)
                   -> (bldr -> ct)
                   -> Streamly.Fold.Fold m a ct
streamlySerializeF encodeOne bldrToCT = Streamly.Fold.Fold step initial extract where
  step (Accum n b) !a = return $ Accum (n + 1) (b <> encodeOne a)
  initial = return $ Accum 0 mempty
  extract (Accum n b) = return $ bldrToCT $ encodeOne (fromIntegral @Int @Word.Word64 n) <> b
{-# INLINEABLE streamlySerializeF #-}

toCT :: BSB.Builder -> Int -> Streamly.Memory.Array.Array Word8
toCT bldr n = bsbToCT $ encodeBSB (fromIntegral @Int @Word.Word64 n) <> bldr

streamlySerializeF2 :: forall c bldr m a ct.(Monad m, Monoid bldr, c a, c Word.Word64)
                   => (forall b. c b => b -> bldr)
                   -> (bldr -> ct)
                   -> Streamly.Fold.Fold m a ct
streamlySerializeF2 encodeOne bldrToCT =
  let fBuilder = Streamly.Fold.Fold step initial return where
        step !b !a = return $ b <> encodeOne a
        initial = return mempty
      toCT' bldr n = bldrToCT $ encodeOne (fromIntegral @Int @Word.Word64 n) <> bldr
  in toCT' <$> fBuilder <*> Streamly.Fold.length
--        extract (Accum n b) = return $ bldrToCT $ encodeOne (fromIntegral @Int @Word.Word64 n) <> b
{-# INLINEABLE streamlySerializeF2 #-}

type SmallRow = [PUMSYEAR, PUMSPERWT, PUMSSTATEFIP, PUMSREGION, PUMSPUMA, PUMSMETRO, PUMSDENSITY, PUMSAGE]

testInIO :: IO ()
testInIO = do
  let pumsCSV = "example_data/acs100k.csv"
  putTextLn "Testing File.toBytes..."
  let rawBytesS =  Streamly.File.toBytes pumsCSV
  rawBytes <-  Streamly.fold Streamly.Fold.length rawBytesS
  putTextLn $ "raw PUMS data has " <> show rawBytes <> " bytes."
  putTextLn "Testing readTable..."
  let sPUMSRawRows :: Streamly.SerialT IO PUMS_Raw
        = FStreamly.readTableOpt Frames.defaultParser pumsCSV
  iRows <-  Streamly.fold Streamly.Fold.length sPUMSRawRows
  putTextLn $ "raw PUMS data has " <> (T.pack $ show iRows) <> " rows."
  putTextLn "Testing Frames.Streamly.inCoreAoS:"
  fPums <- FStreamly.inCoreAoS sPUMSRawRows
  putTextLn $ "raw PUMS frame has " <> show (FL.fold FL.length fPums) <> " rows."

  putTextLn "frame to vec"
  vPostFrame :: V.Vector PUMS_Raw <- FL.foldM FL.vectorM fPums
  putTextLn $ "vPostFrame has " <> show (V.length vPostFrame) <> " rows"

  putTextLn "frame to array"
  aPostFrame :: Streamly.Data.Array.Array PUMS_Raw <- Streamly.fold Streamly.Data.Array.write $ Streamly.fromFoldable fPums
  putTextLn $ "aPostFrame has " <> show (Streamly.Data.Array.length aPostFrame) <> " rows"

{-
  -- Previous goes up to 28MB, looks like via doubling.  Then to 0 (collects fPums after counting?)
  -- This one then climbs to 10MB, rows are smaller.  No large leaks.
  let f :: PUMS_Raw -> F.Record SmallRow
      f !x = F.rcast x
  putTextLn "Testing Frames.Streamly.inCoreAoS with row transform:"
  fPums' :: F.FrameRec SmallRow <- FStreamly.inCoreAoS $ Streamly.map f sPUMSRawRows
  putTextLn $ "transformed PUMS frame has " <> show (FL.fold FL.length fPums') <> " rows."
  putTextLn "v1"
--    sDict  = KS.cerealStreamlyDict
  let countFold = runningCountF "reading..." (\n -> "read " <> show (250000 * n) <> " rows") "finished"
      sPUMSRunningCount = Streamly.map f
                          $ Streamly.tapOffsetEvery 250000 250000 countFold sPUMSRawRows
      sPUMSRCToS = Streamly.map FS.toS sPUMSRunningCount

  serializedBytes :: Streamly.Memory.Array.Array Word.Word8  <- Streamly.fold (streamlySerializeF2 @S.Serialize encodeBSB bsbToCT)  sPUMSRCToS
  print $ Streamly.Memory.Array.length serializedBytes

  putTextLn "v4"
  bldr <- Streamly.foldl' (\acc !x -> let b = S.runPut (S.put x) in b `seq` (acc <> BSB.bytes b)) mempty sPUMSRCToS
  print $ BS.length $ BSB.builderBytes bldr

  putTextLn "In stages"
  putTextLn "Word8: "
  w8Array <- Streamly.Data.Array.fromStream $ Streamly.File.toBytes pumsCSV
  putTextLn $ "w8 array is " <> show (Streamly.Data.Array.length w8Array) <> " long."

  putTextLn "To Vector"
  putTextLn "Word8: "
  vWord8 <- streamToVector $ FStreamly.streamWord8 pumsCSV
  putTextLn $ "w8 vector is " <> show (V.length vWord8) <> " long."

  putTextLn "text lines (via vector)"
  vTextLine <- streamToVector $ FStreamly.streamTextLines pumsCSV
  putTextLn $ "vTextLine has " <> show (V.length vTextLine) <> " elements."

  putTextLn "text lines (via mutable vector)"
  mvTextLine <- streamToVector2 100 $ FStreamly.streamTextLines pumsCSV
  putTextLn $ "mvTextLine has " <> show (V.length mvTextLine) <> " elements."

  putTextLn "tokenized (via vector)"
  vTokenized <- streamToVector $ FStreamly.streamTokenized pumsCSV
  putTextLn $ "vTokenized has " <> show (V.length vTokenized) <> " elements."

  putTextLn "parsed (via vector)"
  vParsed :: V.Vector (F.Rec (Strict.Either Text F.:. F.ElField) (F.RecordColumns PUMS_Raw)) <- streamToVector $ FStreamly.streamParsed pumsCSV
  putTextLn $ "vParsed has " <> show (V.length vParsed) <> " elements."

  putTextLn "parsedMaybe (via vector)"
  vParsedMaybe :: V.Vector (F.Rec (Maybe F.:. F.ElField) (F.RecordColumns PUMS_Raw)) <- streamToVector $ FStreamly.streamParsedMaybe pumsCSV
  putTextLn $ "vParsedMaybe has " <> show (V.length vParsedMaybe) <> " elements."
-}
{-
  putTextLn "Copy to boxed array"
  array <- Streamly.Data.Array.fromStream $ sPUMSRunningCount
  putTextLn $ "array has " <> show (Streamly.Data.Array.length array) <> " elements."
-}

streamToVector :: Monad m => Streamly.SerialT m a -> m (V.Vector a)
streamToVector = V.unfoldrM strictUncons

strictUncons :: Monad m => Streamly.SerialT m a -> m (Maybe (a, Streamly.SerialT m a))
strictUncons s = do
  lu <- Streamly.uncons s
  case lu of
    Nothing -> return Nothing
    Just (!a, s') -> return $ Just (a, s')
{-# INLINE strictUncons #-}

streamToVector2 :: (Monad m, Prim.PrimMonad m) => Int -> Streamly.SerialT m a -> m (V.Vector a)
streamToVector2 initialSize s0 = do
  let go curLength curN s v = do
        mNext <- Streamly.uncons s
        case mNext of
          Nothing -> V.unsafeFreeze (VM.unsafeTake curN v)
          Just (!a, s') -> do
            (curLength', v') <- if curN < curLength
                                then return (curLength, v)
                                else VM.grow v (2 * curLength) >>= \v' -> return (2 * curLength, v')
            VM.write v' curN a
            go curLength' (curN + 1) s' v'
  v0 <- VM.new initialSize
  go initialSize 0 s0 v0


runningCountF :: ST.MonadIO m => T.Text -> (Int -> T.Text) -> T.Text -> Streamly.Fold.Fold m a ()
runningCountF startMsg countMsg endMsg = Streamly.Fold.Fold step start done where
  start = ST.liftIO (putText startMsg) >> return 0
  step !n _ = ST.liftIO $ do
    t <- System.Clock.getTime System.Clock.ProcessCPUTime
    putStr $ show t ++ ": "
    putTextLn $ countMsg n
    return (n+1)
  done _ = ST.liftIO $ putTextLn endMsg
