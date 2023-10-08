{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module DemoPaths where

import DayOfWeek

import qualified Paths_Frames_streamly as Paths
import qualified Frames.Streamly.TH as FStreamly
import qualified Frames.Streamly.CSV as FStreamly
import qualified Frames.Streamly.ColumnUniverse as FStreamly
import qualified Frames.Streamly.ColumnTypeable as FStreamly
import Frames.Streamly.OrMissing
import qualified Data.Set as Set
import Data.Vinyl as V

forestFiresPath :: FilePath
forestFiresPath = "forestfires.csv"

forestFiresMissingPath :: FilePath
forestFiresMissingPath = "forestfiresMissing.csv"

cesPath :: FilePath
cesPath = "CES_short.csv"

thPath :: FilePath -> FilePath
thPath x = "./example_data/" ++ x

usePath :: FilePath -> IO FilePath
usePath x =  fmap (\dd -> dd ++ "/" ++ x) Paths.getDataDir

ffBaseRowGen :: FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByName FStreamly.CommonColumns
ffBaseRowGen = (FStreamly.rowGen (thPath forestFiresPath)) { FStreamly.rowTypeName = "ForestFires", FStreamly.tablePrefix = "FF" }

ffColSubsetRowGen :: FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByName FStreamly.CommonColumns
ffColSubsetRowGen = FStreamly.modifyColumnSelector modSelector rowGen
  where
    rowTypeName = "FFColSubset"
    rowGen = (FStreamly.rowGen (thPath forestFiresPath)) { FStreamly.rowTypeName = rowTypeName, FStreamly.tablePrefix = "CS" }
    modSelector = FStreamly.columnSubset (Set.fromList $ fmap FStreamly.HeaderText ["X","Y","month","day","temp","wind"])

ffColSubsetRowGenCat :: FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByName FStreamly.CommonColumnsCat
ffColSubsetRowGenCat = FStreamly.modifyColumnSelector modSelector rowGen
  where
    rowTypeName = "FFColSubsetCat"
    rowGen = (FStreamly.rowGenCat (thPath forestFiresPath)) { FStreamly.rowTypeName = rowTypeName, FStreamly.tablePrefix = "Cat" }
    modSelector = FStreamly.columnSubset (Set.fromList $ fmap FStreamly.HeaderText ["X","Y","month","day","temp","wind"])

ffInferOrMissingRG :: FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByName FStreamly.CommonColumns
ffInferOrMissingRG = setOrMissingWhen $ FStreamly.modifyColumnSelector modSelector rowGen
  where
    setOrMissingWhen = FStreamly.setOrMissingWhen (FStreamly.HeaderText "wind") FStreamly.IfSomeMissing
    modSelector = FStreamly.columnSubset (Set.fromList $ fmap FStreamly.HeaderText ["X","Y","month","day","temp","wind"])
    rowGen = (FStreamly.rowGen (thPath forestFiresMissingPath)) {
      FStreamly.rowTypeName = "FFInferOrMissing"
      , FStreamly.tablePrefix = "IM"
      }


ffInferOrMissingCatRG :: FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByName FStreamly.CommonColumnsCat
ffInferOrMissingCatRG = setOrMissingWhen $ FStreamly.modifyColumnSelector modSelector rowGen
  where
    setOrMissingWhen = FStreamly.setOrMissingWhen (FStreamly.HeaderText "wind") FStreamly.IfSomeMissing
                   . FStreamly.setOrMissingWhen (FStreamly.HeaderText "day") FStreamly.IfSomeMissing
    modSelector = FStreamly.columnSubset (Set.fromList $ fmap FStreamly.HeaderText ["X","Y","month","day","temp","wind"])
    rowGen = (FStreamly.rowGenCat (thPath forestFiresMissingPath)) {
      FStreamly.rowTypeName = "FFInferOrMisingCat"
      , FStreamly.tablePrefix = "IMC"
      }


type TDColumns = [Bool, Int, Double, DayOfWeek, Text]

ffInferTypedDayRG :: FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByName TDColumns
ffInferTypedDayRG = FStreamly.modifyColumnSelector modSelector rg
  where
    rg = FStreamly.RowGen
         FStreamly.allColumnsAsNamed
         "TD"
         FStreamly.defaultSep
         FStreamly.defaultQuotingMode
         "FFInferTypedDay"
         FStreamly.parseableParseHowRec
         1000
         FStreamly.defaultIsMissing
         (\sep -> FStreamly.sTokenized @_ @IO sep FStreamly.defaultQuotingMode (thPath forestFiresPath))
         (thPath forestFiresPath)
    modSelector = FStreamly.columnSubset (Set.fromList $ fmap FStreamly.HeaderText ["X","Y","month","day","temp","wind"])


ffInferTypedDayOrMissingRG :: FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByName TDColumns
ffInferTypedDayOrMissingRG = setOrMissingWhen
                              $ ffInferTypedDayRG { FStreamly.rowTypeName = "FFInferTypedDayOrMissing"
                                                  , FStreamly.tablePrefix = "TDOM"
                                                  , FStreamly.lineReader = \sep -> FStreamly.sTokenized sep FStreamly.defaultQuotingMode (thPath forestFiresPath)

                                                  } where
  setOrMissingWhen = FStreamly.setOrMissingWhen (FStreamly.HeaderText "wind") FStreamly.IfSomeMissing
                     . FStreamly.setOrMissingWhen (FStreamly.HeaderText "day") FStreamly.IfSomeMissing


data Month = Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Sep | Oct | Nov | Dec deriving (Show, Read, Eq, Enum, Bounded)

FStreamly.derivingOrMissingUnboxVectorFor'
  (derivingUnbox
    "Month"
    [t|Month -> Word8|]
    [e|fromIntegral . fromEnum|]
    [e|toEnum . fromIntegral|])
  "Month"
  [e|Jan|]


parseMonthLower :: Text -> Maybe Month
parseMonthLower = \case
  "jan" -> Just Jan
  "feb" -> Just Feb
  "mar" -> Just Mar
  "apr" -> Just Apr
  "may" -> Just May
  "jun" -> Just Jun
  "jul" -> Just Jul
  "aug" -> Just Aug
  "sep" -> Just Sep
  "oct" -> Just Oct
  "nov" -> Just Nov
  "dec" -> Just Dec
  _ -> Nothing

type DayMonthCols = [Bool, Int, Double, DayOfWeek, Month, Text]

dayMonthColsParserHowRec :: FStreamly.ParseHowRec DayMonthCols
dayMonthColsParserHowRec =
  let pph = FStreamly.parseableParseHow
  in pph V.:& pph V.:& pph V.:& pph V.:& FStreamly.simpleParseHow parseMonthLower V.:& pph V.:& V.RNil

ffInferTypedDayMonthRG :: FStreamly.RowGen FStreamly.DefaultStream 'FStreamly.ColumnByName DayMonthCols
ffInferTypedDayMonthRG = FStreamly.modifyColumnSelector modSelector rg
  where
    rg = FStreamly.RowGen
         FStreamly.allColumnsAsNamed
         "TDM"
         FStreamly.defaultSep
         FStreamly.defaultQuotingMode
         "FFInferTypedDayMonth"
         dayMonthColsParserHowRec
         1000
         FStreamly.defaultIsMissing
         (\sep -> FStreamly.sTokenized @_ @IO sep FStreamly.defaultQuotingMode (thPath forestFiresPath))
         (thPath forestFiresPath)
    modSelector = FStreamly.columnSubset (Set.fromList $ fmap FStreamly.HeaderText ["X","Y","month","day","temp","wind"])


{-
cesCols :: Set.Set FStreamly.HeaderText
cesCols = Set.fromList (FStreamly.HeaderText <$> ["year"
                                                 , "case_id"
                                                 , "weight"
                                                 , "weight_cumulative"
                                                 , "st"
                                                 , "dist_up"
                                                 , "gender"
                                                 , "age"
                                                 , "educ"
                                                 , "race"
                                                 , "hispanic"
                                                 , "pid3"
                                                 , "pid7"
                                                 , "pid3_leaner"
                                                 , "vv_regstatus"
                                                 , "vv_turnout_gvm"
                                                 , "voted_rep_party"
                                                 , "voted_pres_08"
                                                 , "voted_pres_12"
                                                 , "voted_pres_16"
                                                 , "voted_pres_20"
                                                 ])

cesRowGenAllCols = (FStreamly.rowGen (thPath cesPath)) { FStreamly.tablePrefix = "CCES"
                                                       , FStreamly.separator   = ","
                                                       , FStreamly.rowTypeName = "CCES"
                                                       }

cesRowGen = FStreamly.modifyColumnSelector colSubset cesRowGenAllCols where
  colSubset = FStreamly.columnSubset cesCols
-}
