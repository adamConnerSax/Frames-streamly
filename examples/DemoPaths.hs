{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}

module DemoPaths where

import DayOfWeek

import qualified Paths_Frames_streamly as Paths
import qualified Frames.Streamly.TH as FStreamly
import qualified Frames.Streamly.CSV as FStreamly
import qualified Frames.Streamly.ColumnUniverse as FStreamly
import qualified Data.Set as Set


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

ffColSubsetRowGen :: FStreamly.RowGen 'FStreamly.ColumnByName FStreamly.CommonColumns
ffColSubsetRowGen = FStreamly.modifyColumnSelector modSelector rowGen
  where
    rowTypeName = "FFColSubset"
    rowGen = (FStreamly.rowGen (thPath forestFiresPath)) { FStreamly.rowTypeName = rowTypeName }
    modSelector = FStreamly.columnSubset (Set.fromList $ fmap FStreamly.HeaderText ["X","Y","month","day","temp","wind"])

ffColSubsetRowGenCat :: FStreamly.RowGen 'FStreamly.ColumnByName FStreamly.CommonColumnsCat
ffColSubsetRowGenCat = FStreamly.modifyColumnSelector modSelector rowGen
  where
    rowTypeName = "FFColSubsetCat"
    rowGen = (FStreamly.rowGenCat (thPath forestFiresPath)) { FStreamly.rowTypeName = rowTypeName, FStreamly.tablePrefix = "Cat" }
    modSelector = FStreamly.columnSubset (Set.fromList $ fmap FStreamly.HeaderText ["X","Y","month","day","temp","wind"])


ffInferOrMissingRG :: FStreamly.RowGen 'FStreamly.ColumnByName FStreamly.CommonColumns
ffInferOrMissingRG = setOrMissingWhen $ FStreamly.modifyColumnSelector modSelector rowGen
  where
    setOrMissingWhen = FStreamly.setOrMissingWhen (FStreamly.HeaderText "wind") FStreamly.IfSomeMissing
    modSelector = FStreamly.columnSubset (Set.fromList $ fmap FStreamly.HeaderText ["X","Y","month","day","temp","wind"])
    rowGen = (FStreamly.rowGen (thPath forestFiresMissingPath)) {
      FStreamly.rowTypeName = "FFInferOrMissing"
      , FStreamly.tablePrefix = "IM"
      }


ffInferOrMissingCatRG :: FStreamly.RowGen 'FStreamly.ColumnByName FStreamly.CommonColumnsCat
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

ffInferTypedDayRG :: FStreamly.RowGen 'FStreamly.ColumnByName TDColumns
ffInferTypedDayRG = FStreamly.modifyColumnSelector modSelector rg
  where
    rg = FStreamly.RowGen
         FStreamly.allColumnsAsNamed
         "TD"
         FStreamly.defaultSep
         "FFInferTypedDay"
         (Proxy :: Proxy TDColumns)
         1000
         FStreamly.defaultIsMissing
         (FStreamly.streamTokenized'  (thPath forestFiresPath))
    modSelector = FStreamly.columnSubset (Set.fromList $ fmap FStreamly.HeaderText ["X","Y","month","day","temp","wind"])

ffInferTypedDayOrMissingRG :: FStreamly.RowGen 'FStreamly.ColumnByName TDColumns
ffInferTypedDayOrMissingRG = setOrMissingWhen
                              $ ffInferTypedDayRG { FStreamly.rowTypeName = "FFInferTypedDayOrMissing"
                                                  , FStreamly.tablePrefix = "TDOM"
                                                  , FStreamly.lineReader =  (FStreamly.streamTokenized'  (thPath forestFiresMissingPath))} where
  setOrMissingWhen = FStreamly.setOrMissingWhen (FStreamly.HeaderText "wind") FStreamly.IfSomeMissing
                     . FStreamly.setOrMissingWhen (FStreamly.HeaderText "day") FStreamly.IfSomeMissing


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
