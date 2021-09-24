{-# LANGUAGE OverloadedStrings #-}
module DemoPaths where

import qualified Paths_Frames_streamly as Paths
import qualified Frames.Streamly.TH as FStreamly
import qualified Frames.ColumnUniverse as Frames

forestFiresPath :: FilePath
forestFiresPath = "forestfires.csv"

thPath :: FilePath -> FilePath
thPath x = "./example_data/" ++ x

usePath :: FilePath -> IO FilePath
usePath x =  fmap (\dd -> dd ++ "/" ++ x) Paths.getDataDir

ffColSubsetRowGen :: FStreamly.RowGen Frames.CommonColumns
ffColSubsetRowGen = (FStreamly.rowGen (thPath forestFiresPath))
                    {
                      FStreamly.headerFilter = Just (`elem` ["X","Y","month","day","temp","wind"])
                    , FStreamly.rowTypeName = "FFColSubset"
                    }
