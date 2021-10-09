{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
--{-# LANGUAGE TypeApplications #-}

module DemoPaths where

import qualified Paths_Frames_streamly as Paths
import qualified Frames.Streamly.TH as FStreamly
import qualified Frames.ColumnUniverse as Frames
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

ffColSubsetRowGen :: FStreamly.RowGen 'FStreamly.ColumnByName Frames.CommonColumns
ffColSubsetRowGen = FStreamly.modifyColumnSelector modSelector rowGen
  where
    rowTypeName = "FFColSubset"
    rowGen = (FStreamly.rowGen (thPath forestFiresPath)) { FStreamly.rowTypeName = rowTypeName }
    modSelector = FStreamly.columnSubset (Set.fromList $ fmap FStreamly.HeaderText ["X","Y","month","day","temp","wind"])

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
