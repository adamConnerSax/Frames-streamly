{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}
module DemoPaths where

import qualified Paths_Frames_streamly as Paths
import qualified Frames.Streamly.TH as FStreamly
import qualified Frames.ColumnUniverse as Frames
import qualified Data.Set as Set
import qualified Data.Map as Map
import qualified  Data.Text as T

forestFiresPath :: FilePath
forestFiresPath = "forestfires.csv"

forestFiresNoHeaderPath :: FilePath
forestFiresNoHeaderPath = "ForestFiresNoHeader.csv"

thPath :: FilePath -> FilePath
thPath x = "./example_data/" ++ x

usePath :: FilePath -> IO FilePath
usePath x =  fmap (\dd -> dd ++ "/" ++ x) Paths.getDataDir

ffColSubsetRowGen :: FStreamly.RowGen 'FStreamly.ColumnByName Frames.CommonColumns
ffColSubsetRowGen = FStreamly.modifyColumnHandler modHandler rowGen
  where
    rowTypeName = "FFColSubset"
    rowGen = (FStreamly.rowGen (thPath forestFiresPath)) { FStreamly.rowTypeName = rowTypeName }
    modHandler = FStreamly.columnSubset (Set.fromList ["X","Y","month","day","temp","wind"])

ffNoHeaderRowGen :: FStreamly.RowGen 'FStreamly.ColumnByPosition Frames.CommonColumns
ffNoHeaderRowGen = FStreamly.modifyColumnHandler modHandler rowGen
  where
    rowTypeName = "FFNoHeader"
    rowGen = (FStreamly.rowGen (thPath forestFiresNoHeaderPath)) { FStreamly.rowTypeName = rowTypeName }
    modHandler = const $ FStreamly.noHeaderColumnsNumbered "Col"

ffIgnoreHeaderRowGen :: FStreamly.RowGen 'FStreamly.ColumnByPosition  Frames.CommonColumns
ffIgnoreHeaderRowGen = FStreamly.modifyColumnHandler modHandler rowGen
  where
    rowTypeName = "FFIgnoreHeader"
    rowGen = (FStreamly.rowGen (thPath forestFiresPath)) { FStreamly.rowTypeName = rowTypeName }
    modHandler = const $ FStreamly.namesGiven True $ ("Col" <>) . T.pack . show <$> ([0..12] :: [Int])

ffIgnoreHeaderChooseNamesRowGen :: FStreamly.RowGen 'FStreamly.ColumnByPosition  Frames.CommonColumns
ffIgnoreHeaderChooseNamesRowGen = FStreamly.modifyColumnHandler modHandler rowGen
  where
    rowTypeName = "FFIgnoreHeaderChooseNames"
    rowGen = (FStreamly.rowGen (thPath forestFiresPath)) { FStreamly.rowTypeName = rowTypeName }
    modHandler = const
                 $ FStreamly.namedColumnNumberSubset True
                 $ Map.fromList [(0, "X"), (1,"Y"), (2, "Month"), (3, "Day"), (8, "Temp"), (10, "Wind")]
