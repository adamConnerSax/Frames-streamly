{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module Paths where

import qualified Paths_Frames_streamly as Paths
import qualified Frames.Streamly.TH as FStreamly
import qualified Frames.Streamly.CSV as FStreamly hiding (quotingMode)
import qualified Frames.Streamly.ColumnTypeable as FStreamly
import qualified Frames.Streamly.ColumnUniverse as FStreamly
import qualified Frames.Streamly.OrMissing as FStreamly
import Frames.Streamly.Streaming.Class (sTokenized)
import qualified Frames.Streamly.Streaming.Pipes as StreamP
import qualified Frames.Streamly.Streaming.Streamly as StreamS

import qualified Data.Set as Set
import qualified Data.Map as Map
import qualified Data.Text as T
import qualified Data.Vinyl as V
import qualified Frames
import qualified Frames.TH as Frames

forestFiresPrefix :: FilePath
forestFiresPrefix = "forestFires"

forestFiresPath :: FilePath
forestFiresPath = "forestFires.csv"

forestFiresNoHeaderPath :: FilePath
forestFiresNoHeaderPath = "forestFiresNoHeader.csv"

forestFiresFewerColsPath :: FilePath
forestFiresFewerColsPath = "forestFiresFewerCols.csv"

--thPath :: FilePath -> FilePath
--thPath x = "./example_data/" ++ x

usePath :: FilePath -> IO FilePath
usePath x =  fmap (\dd -> dd ++ "/" ++ x) Paths.getDataDir


ffRowGenIO :: IO (Frames.RowGen Frames.CommonColumns)
ffRowGenIO = do
  fp <- usePath forestFiresPath
  pure $ (Frames.rowGen fp) { Frames.rowTypeName = "FF" }

ffNewRowGenIO :: IO (FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByName Frames.CommonColumns)
ffNewRowGenIO = do
  fp <- usePath forestFiresPath
  pure $ (FStreamly.rowGen fp) { FStreamly.rowTypeName = "FFNew" }

ffNewRowGenPIO :: FilePath -> IO (FStreamly.RowGen StreamP.PipeStream 'FStreamly.ColumnByName Frames.CommonColumns)
ffNewRowGenPIO fp = do
  ffp <- usePath forestFiresPath
  let rg = FStreamly.rowGen ffp
  pure $ rg  { FStreamly.rowTypeName = "FFNew"
             , FStreamly.lineReader = \sep -> sTokenized sep (FStreamly.quotingMode rg) fp --FStreamly.streamTokenized' @StreamP.PipeStream @IO fp
             }

ffNewRowGenSIO :: FilePath -> IO (FStreamly.RowGen (FStreamly.DefaultStream) 'FStreamly.ColumnByName Frames.CommonColumns)
ffNewRowGenSIO fp = do
  ffp <- usePath forestFiresPath
  let rg = FStreamly.rowGen ffp
  pure  $ rg
    { FStreamly.rowTypeName = "FFNew"
    , FStreamly.lineReader = \sep -> sTokenized sep (FStreamly.quotingMode rg) fp --FStreamly.streamTokenized' @(StreamS.StreamlyStream StreamS.SerialT) @IO fp
    }

ffColSubsetRowGenIO :: FilePath -> IO (FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByName Frames.CommonColumns)
ffColSubsetRowGenIO fp = do
  fp' <- usePath fp
  let
    rowTypeName = "FFColSubset"
    rowGen = (FStreamly.rowGen fp') { FStreamly.rowTypeName = rowTypeName }
    modSelector = FStreamly.columnSubset (Set.fromList $ fmap FStreamly.HeaderText ["X","Y","month","day","temp"])
  pure $ FStreamly.modifyColumnSelector modSelector rowGen

data Mth = Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec deriving (Enum, Bounded)
FStreamly.derivingOrMissingUnboxVectorFor'
  (FStreamly.derivingUnbox  "Mth" [t|Mth -> Word8|] [e|fromIntegral . fromEnum|] [e|toEnum . fromIntegral|])
  "Mth"
  [e|Jan|]
type MthC = "Month" Frames.:-> Mth

parseMth :: Text -> Either Text Mth
parseMth "jan" = Right Jan
parseMth "feb" = Right Feb
parseMth "mar" = Right Mar
parseMth "apr" = Right Apr
parseMth "may" = Right May
parseMth "jun" = Right Jun
parseMth "jul" = Right Jul
parseMth "aug" = Right Aug
parseMth "sep" = Right Sep
parseMth "oct" = Right Oct
parseMth "nov" = Right Nov
parseMth "dec" = Right Dec
parseMth x = Left x

instance FStreamly.Parseable Mth where
  parse t = case parseMth t of
    Left _ -> mzero
    Right p -> return $ FStreamly.Definitely p

data DayOfWeek = Mon | Tue | Wed | Thu | Fri | Sat | Sun deriving (Enum, Bounded)
FStreamly.derivingOrMissingUnboxVectorFor'
  (FStreamly.derivingUnbox  "DayOfWeek" [t|DayOfWeek -> Word8|] [e|fromIntegral . fromEnum|] [e|toEnum . fromIntegral|])
  "DayOfWeek"
  [e|Mon|]

type DayC = "Day" Frames.:-> DayOfWeek

parseDayOfWeek :: Text -> Either Text DayOfWeek
parseDayOfWeek "mon" = Right Mon
parseDayOfWeek "tue" = Right Tue
parseDayOfWeek "wed" = Right Wed
parseDayOfWeek "thu" = Right Thu
parseDayOfWeek "fri" = Right Fri
parseDayOfWeek "sat" = Right Sat
parseDayOfWeek "sun" = Right Sun
parseDayOfWeek x = Left x

instance FStreamly.Parseable DayOfWeek where
  parse t = case parseDayOfWeek t of
    Left _ -> mzero
    Right p -> return $ FStreamly.Definitely p

type ParsedCols = [Bool, Int, Double, Mth, DayOfWeek, Text]


dayMonthColsParserHowRec :: FStreamly.ParseHowRec ParsedCols
dayMonthColsParserHowRec = FStreamly.parseableParseHowRec
{-  let pph = FStreamly.parseableParseHow
  in pph
     V.:& pph
     V.:& pph
     V.:& FStreamly.simpleParseHow (either (const Nothing) Just . parseMth)
     V.:& FStreamly.simpleParseHow (either (const Nothing) Just . parseDayOfWeek)
     V.:& pph
     V.:& V.RNil
-}
ffInferTypedSubsetRGIO :: FilePath -> IO (FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByName ParsedCols)
ffInferTypedSubsetRGIO fp = do
  rg <- ffColSubsetRowGenIO fp
  pure $ rg { FStreamly.columnParsers = dayMonthColsParserHowRec
            , FStreamly.tablePrefix ="P"
            , FStreamly.rowTypeName = "FFInferTyped"
            }
