{-# LANGUAGE OverloadedStrings #-}
--{-# LANGUAGE TypeApplications #-}

module DemoPaths where

import qualified Paths_Frames_streamly as Paths
import qualified Frames.Streamly.TH as FStreamly
import qualified Frames.ColumnUniverse as Frames
import qualified Data.Set as Set
import  Data.Text (Text)

forestFiresPath :: FilePath
forestFiresPath = "forestfires.csv"

thPath :: FilePath -> FilePath
thPath x = "./example_data/" ++ x

usePath :: FilePath -> IO FilePath
usePath x =  fmap (\dd -> dd ++ "/" ++ x) Paths.getDataDir

ffColSubsetRowGen :: FStreamly.RowGen Text Frames.CommonColumns
ffColSubsetRowGen = (FStreamly.rowGen (thPath forestFiresPath))
                    {
                      FStreamly.columnHandler = FStreamly.columnSubset (Set.fromList ["X","Y","month","day","temp","wind"]) FStreamly.allColumnsAsNamed
                    , FStreamly.rowTypeName = "FFColSubset"
                    }
