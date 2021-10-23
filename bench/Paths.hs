{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module Paths where

import qualified Paths_Frames_streamly as Paths
import qualified Frames.Streamly.TH as FStreamly
import qualified Frames.Streamly.OrMissing as FStreamly
import qualified Data.Set as Set
import qualified Data.Map as Map
import qualified  Data.Text as T
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

thPath :: FilePath -> FilePath
thPath x = "./example_data/" ++ x

usePath :: FilePath -> IO FilePath
usePath x =  fmap (\dd -> dd ++ "/" ++ x) Paths.getDataDir


ffRowGen :: Frames.RowGen Frames.CommonColumns
ffRowGen = (Frames.rowGen (thPath forestFiresPath)) { Frames.rowTypeName = "FF" }

ffNewRowGen :: FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByName Frames.CommonColumns
ffNewRowGen = (FStreamly.rowGen (thPath forestFiresPath)) { FStreamly.rowTypeName = "FFNew" }

ffColSubsetRowGen :: FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByName Frames.CommonColumns
ffColSubsetRowGen = FStreamly.modifyColumnSelector modSelector rowGen
  where
    rowTypeName = "FFColSubset"
    rowGen = (FStreamly.rowGen (thPath forestFiresPath)) { FStreamly.rowTypeName = rowTypeName }
    modSelector = FStreamly.columnSubset (Set.fromList $ fmap FStreamly.HeaderText ["X","Y","month","day","temp","wind"])

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
