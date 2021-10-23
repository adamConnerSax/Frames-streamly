{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
module Paths where

import qualified Paths_Frames_streamly as Paths
import qualified Frames.Streamly.TH as FStreamly
import qualified Data.Set as Set
import qualified Data.Map as Map
import qualified  Data.Text as T
import qualified Frames.Streamly.TH as FStreamly
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
