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

  putTextLn "Copy to boxed array"
  array <- Streamly.Data.Array.fromStream $ sPUMSRunningCount
  putTextLn $ "array has " <> show (Streamly.Data.Array.length array) <> " elements."

runningCountF :: ST.MonadIO m => T.Text -> (Int -> T.Text) -> T.Text -> Streamly.Fold.Fold m a ()
runningCountF startMsg countMsg endMsg = Streamly.Fold.Fold step start done where
  start = ST.liftIO (putText startMsg) >> return 0
  step !n _ = ST.liftIO $ do
    t <- System.Clock.getTime System.Clock.ProcessCPUTime
    putStr $ show t ++ ": "
    putTextLn $ countMsg n
    return (n+1)
  done _ = ST.liftIO $ putTextLn endMsg
